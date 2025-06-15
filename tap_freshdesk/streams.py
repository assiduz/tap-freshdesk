"""Stream type classes for tap-freshdesk."""

from __future__ import annotations

from pathlib import Path
import copy
import datetime

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk import Tap, metrics

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
