"""REST client handling, including freshdeskStream base class."""

from __future__ import annotations

from pathlib import Path
import time
from typing import Any, Callable, Iterable, TYPE_CHECKING, Generator

import requests
import logging
from http import HTTPStatus
from urllib.parse import urlparse
from singer_sdk.authenticators import BasicAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.pagination import BasePageNumberPaginator, SinglePagePaginator

if TYPE_CHECKING:
    from requests import Response

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class FreshdeskStream(RESTStream):
    """freshdesk stream class."""

    name: str
    records_jsonpath = "$.[*]"  # Or override `parse_response`.
    primary_keys = ["id"]

    @property
    def backoff_max_tries(self) -> int:
        return 10

    @property
    def path(self) -> str:
        """
        'groups' -> '/groups'
        """
        return f"/{self.name}"

    @property
    def schema_filepath(self) -> Path | None:
        return SCHEMAS_DIR / f"{self.name}.json"

    # OR use a dynamic url_base:
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        domain = self.config["domain"]
        return f"https://{domain}.freshdesk.com/api/v2"

    @property
    def authenticator(self) -> BasicAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return BasicAuthenticator.create_for_stream(
            self,
            username=self.config.get("api_key", ""),
            password="",
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("auth_token")
        return headers

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: Any | None,
    ) -> Any | None:
        """Return a token for identifying next page or None if no more pages.

        Args:
            response: The HTTP ``requests.Response`` object.
            previous_token: The previous page token value.

        Returns:
            The next pagination token.
        """
        # TODO: If pagination is required, return a token which can be used to get the
        #       next page. If this is the final page, return "None" to end the
        #       pagination loop.
        if self.next_page_token_jsonpath:
            all_matches = extract_jsonpath(
                self.next_page_token_jsonpath, response.json()
            )
            first_match = next(iter(all_matches), None)
            next_page_token = first_match
        else:
            next_page_token = response.headers.get("X-Next-Page", None)

        return next_page_token

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
        params: dict = {}
        embeds = self.config.get("embeds")
        if embeds:
            embed_fields = embeds.get(self.name, [])
            if embed_fields:  # i.e. 'stats,company,sla_policy'
                params["include"] = ",".join(embed_fields)
        return params


    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        # TODO: Parse response body and return a set of records.
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def get_new_paginator(self) -> SinglePagePaginator:
        return SinglePagePaginator()


    def backoff_jitter(self, value: float) -> float:
        return value

class FreshdeskPaginator(BasePageNumberPaginator):

    def has_more(self, response: Response) -> bool:
        """
        There is no 'has more' indicator for this stream.
        If there are no results on this page, then this is 'last' page,
        (even though technically the page before was the last, there was no way to tell).
        """
        return len(response.json()) != 0 and self.current_value < 300


class PagedFreshdeskStream(FreshdeskStream):

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
        if next_page_token:
            params["page"] = next_page_token
        if "updated_since" not in context:
            params["updated_since"] = self.get_starting_timestamp(context)
        return params

    def get_new_paginator(self) -> BasePageNumberPaginator:
        return FreshdeskPaginator(start_value=1)
