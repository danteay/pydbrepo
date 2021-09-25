"""Definition of an Entity field as a Class descriptor."""

from datetime import date, datetime
from typing import (Any, AnyStr, Iterable, NoReturn, Optional, Tuple, Type, Union)

from dateutil.parser import parse

from pydbrepo.entity.enum_entity import EnumEntity
from pydbrepo.errors import FieldCastError, FieldTypeError

__all__ = ['Field']


class Field:
    """This Descriptor class is use to define an entity field. It validates field values types
    and make transformations over that values to have a normalized version of it. Field needs
    to be defined outside the constructor to be applied correctly.

    An entity class that implements Field descriptor needs to implement the `named_fields`
    decorator in order that Field descriptors works correctly.

    Example:

    .. code-block:: python
       from pydbrepo import Entity, Field, named_fields

       @named_fields
       class Model(Entity):
           name = Field(type_=str)

    :type type_: Union[Type, Tuple[Type, ...]]
    :param type_: Python Type or Tuple of Python Types that should have the value of the field

    :type cast_to: Type
    :param cast_to: Python Type that should be casted in case the value don't have the correct one.
        This property is used at the same time with `cast_if` param. If `cast_if` is not set, all
        values will be casted to the specified type.

    :type cast_if: Union[Type, Tuple[Type, ...]]
    :param cast_if: Python Type or Tuple of Python Types that describes the type that the field
        should be casted to if it is equal to the given value.

    :type cast_items_to: Type
    :param cast_items_to: Python Type that will be used to cast all items on Iterable object. If
        The stored value is not an iterable object and this property is set, it will cause an
        error.

    :type field: str
    :param field: Name of the field that is attached to the descriptor
    """

    def __init__(
        self,
        type_: Union[Type, Tuple[Type, ...]],
        cast_to: Optional[Type] = None,
        cast_if: Optional[Union[Type, Tuple[Type, ...]]] = None,
        cast_items_to: Optional[Type] = None,
        field: Optional[AnyStr] = None,
    ):
        self.__type = type_
        self.__cast_to = cast_to
        self.__cast_items_to = cast_items_to
        self.__cast_if = cast_if
        self.field = field

    def __set__(self, instance: Any, value: Any) -> NoReturn:
        """Validate and save field value.

        :param instance: Filed owner class
        :param value: Given value to assign
        """

        self.__validate_types(instance, value)
        value = self.__cast_value(instance, value)

        instance.__dict__[self.field] = value

    def __get__(self, instance: Any, owner_type: Type) -> Any:
        """Return saved field value.

        :param instance: Filed owner class
        :param owner_type: Python Type of the owner instance

        :return Any: Stored value
        """

        field = instance.__dict__.get(self.field, None)

        if issubclass(type(field), EnumEntity):
            return field.value

        return field

    def __validate_types(self, instance: Any, value: Any) -> NoReturn:
        """Execute type validation for field value.

        :param instance: Filed owner class
        :param value: Given value to assign

        :raise FieldTypeError: If the value is different from expected types
        """

        if not isinstance(value, self.__type) and value is not None:
            raise FieldTypeError(instance.__class__.__name__, self.field, value, self.__type)

    def __cast_value(self, instance: Any, value: Any) -> Any:
        """Execute cast over given value to convert it into an instance of a specific class.

        :param instance: Filed owner class
        :param value: Un-casted value

        :return Any: Casted value
        """

        if self.__cast_items_to:
            return self.__cast_iterable(instance, value)

        if self.__cast_to:
            return self.__cast_non_iterables(instance, value)

        return value

    def __cast_non_iterables(self, instance: Any, value: Any) -> Any:
        """Cast non iterable object values.

        :param instance: Filed owner class
        :param value: Un-casted value

        :return Any: Casted value

        :raise FieldCastError: In case the Type of the given value do not match with the given
            `cast_if` Type when `cast_if` is different from None.
        """

        if isinstance(value, self.__cast_to) or value is None:
            return value

        if self.__cast_if is not None:
            if isinstance(value, self.__cast_if):
                return self.__handle_cast(self.__cast_to, instance, value)

            raise FieldCastError(
                class_name=instance.__class__.__name__,
                field=self.field,
                value=value,
                cast_to=self.__cast_to,
                current_type=type(value),
                cast_if=self.__cast_if
            )

        return self.__handle_cast(self.__cast_to, instance, value)

    def __cast_iterable(self, instance: Any, value: Iterable) -> Any:
        """Cast iterable object items.

        :param instance: Filed owner class
        :param value: Un-casted value

        :return Any: Casted value
        """

        if self.__cast_items_to is None or not value:
            return value

        items = []

        for item in value:
            if isinstance(item, self.__cast_items_to):
                items.append(item)

            item = self.__handle_cast(self.__cast_items_to, instance, item)

            items.append(item)

        # noinspection PyArgumentList
        return type(value)(items)

    def __handle_cast(self, cast_to: Type, instance: Any, value: Any) -> Any:
        """Return casted value

        :param cast_to: Type to be casted
        :param value: Un-casted value

        :return Any: New instance of the value with the corresponding type

        :raise FieldCastError: In case any cast cause an unexpected exception.
        """

        try:
            # Cast for string dates
            if cast_to == datetime:
                return parse(value)

            if cast_to == date:
                return parse(value).date()

            if 'from_dict' in set(dir(cast_to)):
                return cast_to().from_dict(value)

            return self.__cast_to(value)
        except Exception as error:
            raise FieldCastError(
                class_name=instance.__class__.__name__,
                field=self.field,
                value=value,
                cast_to=cast_to,
                cast_if=self.__cast_if,
                current_type=type(value),
                errors=error
            )
