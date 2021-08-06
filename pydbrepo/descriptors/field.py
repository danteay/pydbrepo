"""Descriptor of an entity field."""

from datetime import date, datetime
from typing import Any, AnyStr, Iterable, Optional, Tuple, Type, Union

from dateutil.parser import parse

from pydbrepo.entity.enum_entity import EnumEntity
from pydbrepo.errors import FieldCastError, FieldTypeError

__all__ = ['Field']


class Field:
    """Class that describes a field and his base validations.

    :param type_: Python type of the field
    :param cast_to: Class that describes the type that the field should be casted to
    :param cast_items_to: Class that will be used to cast all items on Iterable object
    :param cast_if: Class that describes the type that the field should be casted to if it is equal to the given value
    """

    def __init__(
        self,
        type_: Union[Type, Tuple],
        cast_to: Optional[Type] = None,
        cast_if: Optional[Union[Type, Tuple[Type, ...]]] = None,
        cast_items_to: Optional[Type] = None,
        field: Optional[AnyStr] = None,
    ):
        self._type = type_
        self._cast_to = cast_to
        self._cast_items_to = cast_items_to
        self._cast_if = cast_if
        self.field = field

    def __set__(self, instance, value):
        """Validate and save field value."""

        self._validate_types(instance, value)
        value = self._cast_value(instance, value)

        instance.__dict__[self.field] = value

    def __get__(self, instance, owner_type) -> Any:
        """Return saved field value."""

        field = instance.__dict__.get(self.field, None)

        if issubclass(type(field), EnumEntity):
            return field.value

        return field

    def _validate_types(self, instance: Any, value: Any):
        """Execute type validation for field value.

        :param instance: Filed owner class
        :param value: Given value to assign
        :raise FieldTypeError: If the value is different from expected types
        """

        if not isinstance(value, self._type) and value is not None:
            raise FieldTypeError(instance.__class__.__name__, self.field, value, self._type)

    def _cast_value(self, instance: Any, value: Any) -> Any:
        """Execute cast over given value to convert it into an instance of a specific class.

        :param instance: Filed owner class
        :param value: Un-casted value
        :return Any: Casted value
        """

        if self._cast_items_to:
            return self._cast_iterable(instance, value)

        if self._cast_to:
            return self._cast_non_iterables(instance, value)

        return value

    def _cast_non_iterables(self, instance: Any, value: Any) -> Any:
        """Cast non iterable object values.

        :param instance: Filed owner class
        :param value: Un-casted value
        :return Any: Casted value
        """

        if isinstance(value, self._cast_to) or value is None:
            return value

        if self._cast_if is not None:
            if isinstance(value, self._cast_if):
                return self._handle_cast(self._cast_to, instance, value)

            raise FieldCastError(
                class_name=instance.__class__.__name__,
                field=self.field,
                value=value,
                cast_to=self._cast_to,
                current_type=type(value),
                cast_if=self._cast_if
            )

        return self._handle_cast(self._cast_to, instance, value)

    def _cast_iterable(self, instance: Any, value: Iterable) -> Any:
        """Cast iterable object items.

        :param instance: Filed owner class
        :param value: Un-casted value
        :return Any: Casted value
        """

        if self._cast_items_to is None or not value:
            return value

        items = []

        for item in value:
            if isinstance(item, self._cast_items_to):
                items.append(item)

            item = self._handle_cast(self._cast_items_to, instance, item)

            items.append(item)

        return type(value)(items)

    def _handle_cast(self, cast_to: Type, instance: Any, value: Any) -> Any:
        """Return casted value

        :param cast_to: Type to be casted
        :param value: Un-casted value
        :return Any: New instance of the value with the corresponding type
        """

        try:
            # Cast for string dates
            if cast_to == datetime:
                return parse(value)

            if cast_to == date:
                return parse(value).date()

            if 'from_dict' in set(dir(cast_to)):
                return cast_to().from_dict(value)

            return self._cast_to(value)
        except Exception as error:
            raise FieldCastError(
                class_name=instance.__class__.__name__,
                field=self.field,
                value=value,
                cast_to=cast_to,
                cast_if=self._cast_if,
                current_type=type(value),
                errors=error
            )
