"""This are decorators that complement the functionality of the Entity Models and
other tools like descriptors to make his use easier.
"""

from typing import Any

from pydbrepo.descriptors import Field


def named_fields(obj: Any) -> Any:
    """This is a class decorator that search over the properties of a class and check if they
    implement the field descriptor, if so, the name of the property is set to the descriptor
    to store correctly the value. If the property is not a Field descriptor is omitted.

    :param obj: Entity model
    :return Any: Modified Entity model
    """

    for name, attr in obj.__dict__.items():
        if isinstance(attr, Field):
            attr.field = name

    return obj
