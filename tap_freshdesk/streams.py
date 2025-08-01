"""Stream type classes for tap-freshdesk."""

from __future__ import annotations

import base64
import asyncio
import aiohttp
import requests
import datetime

from pathlib import Path
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath
from tap_freshdesk.client import FreshdeskStream, PagedFreshdeskStream
from typing import Optional, Iterable


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class AgentsStream(PagedFreshdeskStream):
    name = "agents"

class CompaniesStream(PagedFreshdeskStream):
    name = "companies"

class TicketFieldsStream(FreshdeskStream):
    name = "ticket_fields"

class GroupsStream(PagedFreshdeskStream):
    name = "groups"

class ContactsStream(PagedFreshdeskStream):
    name = "contacts"
    replication_key = 'updated_at'

class EmailConfigsStream(PagedFreshdeskStream):
    name = "email_configs"

class SlaPoliciesStream(PagedFreshdeskStream):
    name = "sla_policies"

class TicketsStream(PagedFreshdeskStream):
    name = "tickets"
    replication_key = 'created_at'

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        context = context or {}
        params = super().get_url_params(context, next_page_token)
        params["per_page"] = 100
        params["order_by"] = "updated_at"
        params['order_type'] = "asc"
        if next_page_token:
            params["page"] = next_page_token
        if "updated_since" not in context:
            updated_since = self.get_starting_timestamp(context)
            if isinstance(updated_since, datetime.datetime):
                # Convert to ISO 8601 string with 'Z' for UTC
                updated_since = updated_since.strftime("%Y-%m-%dT%H:%M:%SZ")
            params["updated_since"] = updated_since
        embeds = self.config.get("embeds", {})
        embed_fields = embeds.get("tickets", [])
        if embed_fields:
            params["include"] = ",".join(embed_fields)
        return params

    # def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
    #     # Pass ticket_id from this ticket record to child stream context
    #     return {"ticket_id": record["id"]}

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        for record in extract_jsonpath(self.records_jsonpath, input=response.json()):
            # Store ticket_id to tap instance
            if not hasattr(self._tap, "ticket_ids"):
                self._tap.ticket_ids = []
            self._tap.ticket_ids.append(record["id"])
            yield record  


class TicketsConversationsStream(PagedFreshdeskStream):
    """
    A stream that extracts all conversations (emails, notes, replies) related to Freshdesk tickets.

    This stream uses the Freshdesk API endpoint `/tickets/{ticket_id}/conversations`
    to retrieve threaded discussions associated with individual support tickets.

    Key Features:
    - Named as "tickets_conversations" to ensure it runs after the "tickets" stream,
      since Meltano processes streams in alphabetical order.
    - Supports pagination with 100 records per page.
    - Authenticates using Freshdesk Basic Auth via API key.
    - Handles HTTP 429 (rate limiting) with retry logic and configurable backoff.
    - Fetches conversations concurrently across multiple tickets using asyncio.
    - Yields records enriched with `ticket_id` for traceability.
    - Uses `updated_at` as the replication key to support incremental syncs.

    This class assumes a list of ticket IDs is provided by the parent context
    (typically set in the `TicketsStream` or tap configuration).
    """

    name = "tickets_conversations"
    primary_keys = ["id", "ticket_id"]
    replication_key = "updated_at"

    # EpicIssues streams should be invoked once per parent epic:
    # parent_stream_type = TicketsStream
    ignore_parent_replication_keys = True

    # Path is auto-populated using parent context keys:
    path = "/tickets/{ticket_id}/conversations"

    state_partitioning_keys = []
    

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rate_limit_lock = asyncio.Lock()  # Lock to serialize 429 retries

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[int]) -> dict:
        # Override to avoid propagating all query string params from the parent stream,
        # as the child stream's endpoint does not support them.
        # Explicitly specify only the params supported by this stream 
        # which are pagination and page size.
        params = {
            "per_page": 100  # always request 100 items per page
        }
        if next_page_token:
            params["page"] = next_page_token
        else:
            params["page"] = 1  # start with page 1 if no token provided
        return params
    
    # def get_records(self, context: Optional[dict]) -> Iterable[dict]:
    #     ticket_ids = getattr(self._tap, "ticket_ids", [])
    #     for ticket_id in ticket_ids:
    #         context = {"ticket_id": ticket_id}
    #         yield from self.request_records(context)

    @property
    def auth_headers(self) -> dict:
        api_key = self.config.get("api_key")
        if not api_key:
            raise ValueError("API key not configured")
        # Freshdesk Basic Auth: username=api_key, password=empty string
        token = base64.b64encode(f"{api_key}:X".encode("utf-8")).decode("utf-8")
        headers = self.http_headers.copy()
        headers.update({
            "Authorization": f"Basic {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        return headers
    
    async def fetch_conversations_for_ticket(
        self,
        session: aiohttp.ClientSession,
        ticket_id: int,
        semaphore: asyncio.Semaphore,
    ) -> list[dict]:
        results = []
        page = 1
        context = {"ticket_id": ticket_id}
        while True:
            async with semaphore:
                url = self.get_url(context)
                params = self.get_url_params(context, page)
                headers = self.auth_headers
                self.logger.info(f"Fetching ticket {ticket_id} page {page} - URL: {url} Params: {params}")
                data = await self.fetch_with_retries(session, url, headers, params)
                if not data:
                    break
                for item in data:
                    item["ticket_id"] = ticket_id
                    results.append(item)
                if len(data) < 100:
                    break
                page += 1
        return results

    async def fetch_with_retries(
        self,
        session: aiohttp.ClientSession,
        url: str,
        headers: dict,
        params: dict,
        max_retries: int = 5,
    ) -> list[dict]:
        retries = 0
        while retries <= max_retries:
            async with session.get(url, headers=headers, params=params) as resp:
                if resp.status == 429:
                    retries += 1
                    if retries > max_retries:
                        raise Exception(f"Max retries exceeded for {url}")
                    retry_after = resp.headers.get("Retry-After")
                    wait_seconds = int(retry_after) if retry_after and retry_after.isdigit() else 60
                    self.logger.warning(f"Rate limited. Waiting {wait_seconds} seconds before retry #{retries}...")
                    await asyncio.sleep(wait_seconds)
                    continue  # retry the request
                resp.raise_for_status()
                return await resp.json()
        raise Exception(f"Exceeded max retries for {url}")
    
    async def _gather_all_conversations(self, ticket_ids: list[int]) -> list[dict]:
        results = []
        semaphore = asyncio.Semaphore(20)
        async with aiohttp.ClientSession() as session:
            tasks = [
                self.fetch_conversations_for_ticket(session, ticket_id, semaphore)
                for ticket_id in ticket_ids
            ]
            all_conversations = await asyncio.gather(*tasks, return_exceptions=True)
            for result in all_conversations:
                if isinstance(result, Exception):
                    self.logger.error(f"Error fetching conversations: {result}")
                    continue
                results.extend(result)
        return results

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        ticket_ids = getattr(self._tap, "ticket_ids", [])
        if not ticket_ids:
            return
        all_records = asyncio.run(self._gather_all_conversations(ticket_ids))
        for record in all_records:
            yield record
