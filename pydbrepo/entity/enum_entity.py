"""Definition of enum entity."""

from enum import Enum
from typing import AnyStr, List

__all__ = ['EnumEntity']


class EnumEntity(Enum):
    """Enumerable entity that will work as custom db enum types and his values"""

    @classmethod
    def values(cls) -> List[AnyStr]:
        """Return all enum type values.

        :return List:
        """

        def map_enum(value):
            name = str(value).rsplit('.', maxsplit=1)[-1]
            return getattr(cls, name).value

        return list(map(map_enum, cls))

    @classmethod
    def is_valid(cls, value: AnyStr) -> bool:
        """Validate if the given value is valid for the Enum type.

        :param value: Value to be compared
        :return bool: Assertion
        """

        try:
            _ = cls(value)
            return True
        except ValueError:
            return False
