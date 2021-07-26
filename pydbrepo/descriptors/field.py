"""Descriptor of an entity field."""

import operator
from datetime import date, datetime
from typing import Any, AnyStr, Optional, Tuple, Type, Union

from dateutil.parser import parse

from pydbrepo.errors import FieldCastError, FieldTypeError, FieldValueError

__all__ = ['Field']


class Field:
    """Class that describes a field and his base validations.

    :param type_: Python type of the field
    :param exp_values: Class that describes the expected values for the field (should extend from EnumEntity)
    :param cast_to: Class that describes the type that the field should be casted to
    :param cast_if: Class that describes the type that the field should be casted to if it is equal to the given value
    :param name: Field name
    """

    def __init__(
        self,
        type_: Union[Type, Tuple],
        exp_values: Optional[Type] = None,
        cast_to: Optional[Type] = None,
        cast_if: Optional[Type] = None,
        name: Optional[AnyStr] = None
    ):
        self._value = None
        self._type = type_
        self._exp_values = exp_values
        self._cast_to = cast_to
        self._cast_if = cast_if
        self._name = name

    def __set__(self, instance, value):
        """Validate and save field value."""

        if self._exp_values is not None:
            self._validate_expected_values(instance, value)

        self._validate_types(instance, value)

        self._value = self._cast_value(instance, value)

    def __get__(self, instance, owner) -> Any:
        """Return saved field value."""

        return self._value

    def __lt__(self, other: Any) -> bool:
        """Lower than comparison."""
        return operator.__lt__(self._value, other)

    def __le__(self, other: Any) -> bool:
        """Lower or equal comparison."""
        return operator.__le__(self._value, other)

    def __gt__(self, other: Any) -> bool:
        """Grater than comparison."""
        return operator.__gt__(self._value, other)

    def __ge__(self, other: Any) -> bool:
        """Grater or equal comparison"""
        return operator.__ge__(self._value, other)

    def __eq__(self, other: Any) -> bool:
        """Equals to comparison."""
        return operator.__eq__(self._value, other)

    def __ne__(self, other) -> bool:
        """Different of comparison."""
        return operator.__ne__(self._value, other)

    def __neg__(self):
        """Negation operation"""

        return operator.__neg__(self._value)

    def __add__(self, other: Any) -> Any:
        return operator.__add__(self._value, other)

    def __sub__(self, other: Any) -> Any:
        return operator.__sub__(self._value, other)

    def __mul__(self, other: Any) -> Any:
        return operator.__mul__(self._value, other)

    def __divmod__(self, other: Any) -> Any:
        return operator.__mod__(self._value, other)

    def __truediv__(self, other: Any) -> Any:
        return operator.__truediv__(self._value, other)

    def __floordiv__(self, other: Any) -> Any:
        return operator.__floordiv__(self._value, other)

    def _validate_expected_values(self, instance: Any, value: Any):
        """Execute expected values validation for the given field value.

        :param instance: Filed owner class
        :param value: Field value
        :raise FieldValueError: If the given value is not expected
        """

        if self._exp_values.is_valid(value) and value is not None:
            raise FieldValueError(instance.__class__.__name__, self._name, value, self._exp_values)

    def _validate_types(self, instance: Any, value: Any):
        """Execute type validation for field value.

        :param instance: Filed owner class
        :param value: Given value to assign
        :raise FieldTypeError: If the value is different from expected types
        """

        if not isinstance(value, self._type):
            raise FieldTypeError(instance.__class__.__name__, self._name, value, self._type)

    def _cast_value(self, instance: Any, value: Any) -> Any:
        """Execute cast over given value to convert it into an instance of a specific class.

        :param instance: Filed owner class
        :param value: Un-casted value
        :return Any: Casted value
        """

        if self._cast_to is None:
            return value

        if self._cast_if is not None:
            if isinstance(value, self._cast_if):
                return self._handle_cast(instance, value)

            raise FieldCastError(
                class_name=instance.__class__.__name__,
                field=self._name,
                value=value,
                cast_to=self._cast_to,
                current_type=type(value),
                cast_if=self._cast_if
            )

        return self._handle_cast(instance, value)

    def _handle_cast(self, instance: Any, value: Any) -> Any:
        """Return casted value

        :param value: Un-casted value
        :return Any: New instance of the value with the corresponding type
        """

        try:
            # Cast for string dates
            if self._cast_to in {date, datetime}:
                return parse(value)

            return self._cast_to(value)
        except Exception as error:
            raise FieldCastError(
                class_name=instance.__class__.__name__,
                field=self._name,
                value=value,
                cast_to=self._cast_to,
                cast_if=self._cast_if,
                current_type=type(value),
                errors=error
            )
