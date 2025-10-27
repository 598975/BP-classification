from typing import Any, TypeVar, cast
import voluptuous as vol

from .selector import validate_selector
from .config_validation import url

from .const import (
    CONF_DEFAULT,
    CONF_DESCRIPTION,
    CONF_DOMAIN,
    CONF_NAME,
    CONF_SELECTOR,
    CONF_AUTHOR,
    CONF_BLUEPRINT,
    CONF_HOMEASSISTANT,
    CONF_INPUT,
    CONF_MIN_VERSION,
    CONF_SOURCE_URL,
    CONF_TRIGGER,
    CONF_ACTION,
)

_T = TypeVar("_T")


def version_validator(value: Any) -> str:
    """Validate a Home Assistant version."""
    if not isinstance(value, str):
        raise vol.Invalid("Version needs to be a string")

    parts = value.split(".")

    if len(parts) != 3:
        raise vol.Invalid("Version needs to be formatted as {major}.{minor}.{patch}")

    try:
        [int(p) for p in parts]
    except ValueError:
        raise vol.Invalid(
            "Major, minor and patch version needs to be an integer"
        ) from None

    return value


def match_all(value: _T) -> _T:
    """Validate that matches all values."""
    return value


BLUEPRINT_INPUT_SCHEMA = vol.Schema(
    {
        vol.Optional(CONF_NAME): str,
        vol.Optional(CONF_DESCRIPTION): str,
        vol.Optional(CONF_DEFAULT): match_all,
        # TODO: restrict to valid selector types
        # To do this, define all of selectors in utils/blueprint/selector.py and register them into SELECTORS
        # https://github.com/home-assistant/core/blob/05b23c2e7b20a7d81b73b24a7385736c45e76d23/homeassistant/helpers/selector.py
        vol.Optional(CONF_SELECTOR): vol.Any(dict, validate_selector),
    }
)

BLUEPRINT_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_BLUEPRINT): vol.Schema(
            {
                vol.Required(CONF_NAME): str,
                vol.Optional(CONF_DESCRIPTION): str,
                vol.Required(CONF_DOMAIN): str,
                vol.Optional(CONF_SOURCE_URL): url,
                vol.Optional(CONF_AUTHOR): str,
                vol.Optional(CONF_HOMEASSISTANT): {
                    vol.Optional(CONF_MIN_VERSION): version_validator
                },
                vol.Optional(CONF_INPUT, default=dict): {
                    str: vol.Any(
                        None,
                        BLUEPRINT_INPUT_SCHEMA,
                    )
                },
            }
        ),
        vol.Required(CONF_TRIGGER): vol.Any(dict, list),
        vol.Required(CONF_ACTION): vol.Any(dict, list),
    },
    extra=vol.ALLOW_EXTRA,
)
