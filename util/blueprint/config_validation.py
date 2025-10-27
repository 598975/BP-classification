from typing import Any, cast
from enum import StrEnum
from urllib.parse import urlparse
import voluptuous as vol


class UrlProtocolSchema(StrEnum):
    """Valid URL protocol schema values."""

    HTTP = "http"
    HTTPS = "https"
    HOMEASSISTANT = "homeassistant"


EXTERNAL_URL_PROTOCOL_SCHEMA_LIST = frozenset(
    {UrlProtocolSchema.HTTP, UrlProtocolSchema.HTTPS}
)


def url(
    value: Any,
    _schema_list: frozenset[UrlProtocolSchema] = EXTERNAL_URL_PROTOCOL_SCHEMA_LIST,
) -> str:
    """Validate an URL."""
    url_in = str(value)

    if urlparse(url_in).scheme in _schema_list:
        return cast(str, vol.Schema(vol.Url())(url_in))

    raise vol.Invalid("invalid url")
