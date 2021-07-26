"""Field errors."""

from typing import Any, AnyStr, Iterable, Optional, Type, Union

__all__ = [
    'FieldValueError',
    'FieldTypeError',
    'FieldCastError',
]


class FieldValueError(ValueError):
    """Exception for value verification error in a entity field.

    :param class_name: Name of the entity class
    :param field: Name of the validated field
    :param value: Validated value
    :param expected: Expected value or values as EnumEntity object or other formats
    """

    def __init__(self, class_name: AnyStr, field: AnyStr, value: Any, expected: Any):
        super().__init__(
            f'{class_name}.{field}({value}) should be one of the next values: '
            f'{self._get_expected_values(expected)}',
        )

    @staticmethod
    def _get_expected_values(expected: Any) -> AnyStr:
        """Obtain expected values from EnumEntity class or return the passed values.

        :param expected: Passed expected values
        :return AnyStr: String representation of the expected values
        """

        values = getattr(expected, 'values', None)

        if values is not None and type(values).__name__ in {'method', 'function'}:
            return str(values())

        return str(expected)


class FieldTypeError(TypeError):
    """Exception for Entity field type validation error.

    :param class_name: Name of the entity class
    :param field: Name of the validated field
    :param value: Validated value
    :param expected_types: Expected type or an iterable object with the expected types of the value
    """

    def __init__(self, class_name: AnyStr, field: AnyStr, value: Any, expected_types: Union[Type, Iterable[Type]]):
        super().__init__(
            f'{class_name}.{field}({value}) should be type(s) {self._types_to_str(expected_types)}: '
            f'given \'{type(value).__name__}\'',
        )

    @staticmethod
    def _types_to_str(expected_types: Union[Type, Iterable[Type]]) -> AnyStr:
        """Parse the expected types and  show his str names.

        :param expected_types: Type or tuple of types
        :return AnyStr: Text of expected types
        """

        if isinstance(expected_types, type):
            return f'{expected_types.__name__}'

        if isinstance(expected_types, tuple):
            names = []

            for exp_type in expected_types:
                names.append(FieldTypeError._types_to_str(exp_type))

            return str(names)

        return type(expected_types).__name__


class FieldCastError(Exception):
    """Exception for Entity field casting error.

    :param class_name: Name of the entity class
    :param field: Name of the validated field
    :param value: Validated value
    :param cast_to: Target type to be casted
    :param current_type: Current type instance of the given value
    :param cast_if: Expected value to apply the cast
    :param errors: Collected exceptions
    """

    def __init__(
        self,
        class_name: AnyStr,
        field: AnyStr,
        value: Any,
        cast_to: Type,
        current_type: Type,
        cast_if: Optional[Type] = None,
        errors: Optional[Exception] = None
    ):
        super().__init__(self._build_error_message(class_name, field, value, cast_to, current_type, cast_if), errors)

    @staticmethod
    def _build_error_message(
        class_name: AnyStr,
        field: AnyStr,
        value: Any,
        cast_to: Type,
        current_type: Type,
        cast_if: Optional[Type] = None
    ) -> AnyStr:
        """Build message according passed params.

        :param class_name: Name of the entity class
        :param field: Name of the validated field
        :param value: Validated value
        :param cast_to: Target type to be casted
        :param current_type: Current type instance of the given value
        :param cast_if: Expected value to apply the cast
        """

        if cast_if is not None:
            return f"{class_name}.{field}({value}) can't be casted from {current_type.__name__} to " \
                   f"{cast_to.__name__}, expected {cast_if.__name__} instead."

        return f"{class_name}.{field}({value}) can't be casted from {current_type.__name__} to {cast_to.__name__}."
