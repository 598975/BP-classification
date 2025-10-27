from typing import Any, TypeVar, cast, Generic
import voluptuous as vol
from collections.abc import Callable, Mapping, Hashable

_T = TypeVar("_T")
_KT = TypeVar("_KT", bound=Hashable)
_VT = TypeVar("_VT", bound=Callable[..., Any])


class Selector(Generic[_T]):
    """Base class for selectors."""

    CONFIG_SCHEMA: Callable
    config: _T
    selector_type: str

    def __init__(self, config: Mapping[str, Any] | None = None) -> None:
        """Instantiate a selector."""
        # Selectors can be empty
        if config is None:
            config = {}

        self.config = self.CONFIG_SCHEMA(config)

    def serialize(self) -> dict[str, dict[str, _T]]:
        """Serialize Selector for voluptuous_serialize."""
        return {"selector": {self.selector_type: self.config}}


class Registry(dict[_KT, _VT]):
    """Registry of items."""

    def register(self, name: _KT) -> Callable[[_VT], _VT]:
        """Return decorator to register item with a specific name."""

        def decorator(func: _VT) -> _VT:
            """Register decorated function."""
            self[name] = func
            return func

        return decorator


SELECTORS: Registry[str, type[Selector]] = Registry()


def _get_selector_class(config: Any) -> type[Selector]:
    """Get selector class type."""
    if not isinstance(config, dict):
        raise vol.Invalid("Expected a dictionary")

    if len(config) != 1:
        raise vol.Invalid(f"Only one type can be specified. Found {', '.join(config)}")

    selector_type: str = list(config)[0]

    if (selector_class := SELECTORS.get(selector_type)) is None:
        raise vol.Invalid(f"Unknown selector type {selector_type} found")

    return selector_class


def validate_selector(config: Any) -> dict:
    """Validate a selector."""
    selector_class = _get_selector_class(config)
    selector_type = list(config)[0]

    # Selectors can be empty
    if config[selector_type] is None:
        return {selector_type: {}}

    return {
        selector_type: cast(dict, selector_class.CONFIG_SCHEMA(config[selector_type]))
    }
