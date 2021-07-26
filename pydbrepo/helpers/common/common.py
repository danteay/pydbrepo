"""Common helpers."""

from uuid import UUID
from typing import Any


def handle_extra_types(value: Any) -> Any:
    """Convert unhandled types to a default value.
    :param value: Value to be converted
    :return Any: converted value
    """

    if isinstance(value, UUID):
        return str(value)

    return value
