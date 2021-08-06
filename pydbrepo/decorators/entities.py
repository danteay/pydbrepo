"""entity decorators."""

from typing import Any

from pydbrepo.descriptors import Field


def named_fields(obj: Any) -> Any:
    """Set Name to Entity fields.

    :param obj: Entity model
    :return Any: Modified entity model
    """

    for name, attr in obj.__dict__.items():
        if isinstance(attr, Field):
            attr.field = name

    return obj
