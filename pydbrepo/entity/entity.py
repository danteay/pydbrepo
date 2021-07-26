"""Entity is a data abstraction, this is not necessary a Table representation, but also
is a data representation of a row returned from any query.
"""

from typing import Any, AnyStr, Dict, List, Tuple

from pydbrepo.errors import SerializationError

__all__ = ['Entity']


class Entity:
    """Entity class definition"""

    def to_dict(self, skip_none: bool = True) -> Dict[AnyStr, Any]:
        """Serialize all object data into a dict.

        :param skip_none: If True, skip all properties of the final result if the value of the properties are None
        :return dict: Serialized data
        """

        data = {}

        for key in set(self.__dir__()):
            skip, value = self._process_dict_values(key, skip_none)

            if skip:
                continue

            data[key] = value

        return data

    def _process_dict_values(self, key: AnyStr, skip_none: bool) -> Tuple[bool, Any]:
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

        value = self._get_property_dict_value(key)

        if value is None and skip_none:
            return True, None

        return False, value

    def _get_property_dict_value(self, name: AnyStr) -> Any:
        """Get the dict value of a model property.

        :param name: Property name
        :return Any: Property dict value
        """

        value = getattr(self, name, None)

        if isinstance(value, Entity):
            value = value.to_dict()

        return value

    @classmethod
    def from_dict(cls, data: Dict[AnyStr, Any]) -> Any:
        """Create instance from current dict data.

        :param data: Data to be loaded
        :return Person:
        """

        instance = cls()
        keys = instance.to_dict(skip_none=False).keys()

        for key in data.keys():
            # Validate if the name starts with underscore and remove it from the name
            if key[:1] == '_':
                key = key[1:]

            if key in keys:
                setattr(instance, key, data[key])

        return instance

    @classmethod
    def from_record(cls, fields: List[AnyStr], record: Tuple[Any]) -> Any:
        """Create an instance from a tuple given by the database driver.
        :param fields: List of field names to serialize
        :param record: DB record data in tuple format
        :return: Instance object
        """

        if len(fields) != len(record):
            raise SerializationError(f'expected fields: {len(fields)} got: {len(record)}', )

        data = {}

        for index, _ in enumerate(fields):
            data[fields[index]] = record[index]

        return cls.from_dict(data)

    def __str__(self) -> AnyStr:
        """String conversion definition."""

        return str(self.to_dict(skip_none=False))

    def __repr__(self) -> AnyStr:
        """Entity representation."""

        return f"{self.__class__.__name__}({self.to_dict(skip_none=False)})"
