"""Entity is a data abstraction, this is not necessary a Table representation, but also
is a data representation of a row returned from any query.
"""

from typing import (Any, AnyStr, Dict, Iterable, List, Mapping, Set, Tuple, Union)

from pydbrepo.errors import SerializationError

__all__ = ['Entity']


class Entity:
    """Entity class definition"""

    def __init__(self):
        self.__dict__ = self.to_dict(skip_none=False)

    def to_dict(self, skip_none: bool = True) -> Dict[AnyStr, Any]:
        """Serialize all object data into a dict.

        :param skip_none: If True, skip all properties of the final result if the value of the properties are None
        :return dict: Serialized data
        """

        data = {}

        for key in set(self.__dir__()):
            if key is None:
                continue

            skip, value = self.__process_dict_values(key, skip_none)

            if skip:
                continue

            data[key] = value

        return data

    def __process_dict_values(self, key: AnyStr, skip_none: bool) -> Tuple[bool, Any]:
        """Process property values to insert on dict conversion.

        :param key: Property name
        :param skip_none: flak to skip none properties
        :return Tuple[bool, Any]: (skip_property, property_value)
        """

        if key[:1] == '_':
            return True, None

        name = type(getattr(self, key, '')).__name__

        if name in {'method', 'function'}:
            return True, None

        value = self.__get_property_dict_value(key)

        if value is None and skip_none:
            return True, None

        return False, value

    def __get_property_dict_value(self, name: AnyStr) -> Any:
        """Get the dict value of a model property.

        :param name: Property name
        :return Any: Property dict value
        """

        value = getattr(self, name, None)
        return self.__property_to_dict(value)

    def __property_to_dict(self, value: Any) -> Any:
        """Convert any value of a property into hist to_dict function equivalent.

        :param value: Property value
        :return Any: Converted value to dict or corresponding value
        """

        if isinstance(value, Entity):
            value = value.to_dict()

        value_type = type(value)

        if issubclass(value_type, Iterable) and not isinstance(value, Mapping) and value_type != str:
            value = self.__items_to_dict(value)

        return value

    def __items_to_dict(self, value: Iterable) -> Iterable:
        """Convert to dict all the items of an iterable object.

        :param value: Iterable object to ve converted
        :return Iterable: Iterable object with casted items to dict
        """

        data = []

        for item in value:
            item = self.__property_to_dict(item)
            data.append(item)

        return type(value)(data)

    @classmethod
    def from_dict(cls, data: Dict[AnyStr, Any]) -> Any:
        """Create instance from current dict data.

        :param data: Data to be loaded
        :return Person:
        """

        instance = cls()
        keys = set(cls.__dict__.keys())

        for key, value in data.items():
            # skip keys that start with __
            if len(key) >= 3 and key[:2] == '__':
                continue

            # Validate if the name starts with underscore and remove it from the name
            if key[:1] == '_':
                key = key[1:]

            if key in keys:
                setattr(instance, key, value)

        return instance

    @classmethod
    def from_record(cls, fields: Union[List[Any], Set[Any], Tuple[Any, ...]], record: Tuple[Any, ...]) -> Any:
        """Create an instance from a tuple given by the database driver.

        :param fields: List of field names to serialize
        :param record: DB record data in tuple format
        :return: Instance object
        """

        if len(fields) != len(record):
            raise SerializationError(f'expected fields: {len(fields)} got: {len(record)}', )

        data = {}

        for index, field in enumerate(fields):
            data[field] = record[index]

        return cls().from_dict(data)

    def __str__(self) -> AnyStr:
        """String conversion definition."""

        return str(self.to_dict(skip_none=False))

    def __repr__(self) -> AnyStr:
        """Entity representation."""

        return f"{self.__class__.__name__}({self.to_dict(skip_none=False)})"
