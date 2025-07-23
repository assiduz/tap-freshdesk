"""Stream type classes for tap-freshdesk."""

from __future__ import annotations

import datetime
from pathlib import Path
from singer_sdk import typing as th  # JSON Schema typing helpers
from tap_freshdesk.client import FreshdeskStream, PagedFreshdeskStream


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

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        # Pass ticket_id from this ticket record to child stream context
        return {"ticket_id": record["id"]}

class ConversationsStream(PagedFreshdeskStream):

    name = "conversations"
    primary_keys = ["id", "ticket_id"]
    replication_key = "updated_at"

    # EpicIssues streams should be invoked once per parent epic:
    parent_stream_type = TicketsStream
    ignore_parent_replication_keys = True

    # Path is auto-populated using parent context keys:
    path = "/tickets/{ticket_id}/conversations"

    state_partitioning_keys = []
    
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