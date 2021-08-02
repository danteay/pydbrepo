"""Abstract repository definition hat can be used to create other implementations."""

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, AnyStr, Dict, List, NoReturn, Optional, Type, Union

from pydbrepo.drivers.driver import Driver
from pydbrepo.entity import Entity

__all__ = ['Repository']


class Repository(ABC):
    """Abstract repository class.

    :param driver: Database driver implementation
    :param entity: Class type that should be handled by the repository
    :param log_level: Logging level
    :param debug: Flag for debug mode
    :param auto_timestamps: Flag to insert timestamps on configured created_at and updated_at fields
    :param created_at: Name for created_at timestamp field
    :param updated_at: Name for updated_at timestamp field
    """

    def __init__(
        self,
        driver: Driver,
        entity: Optional[Type] = None,
        log_level: Optional[int] = None,
        debug: Optional[bool] = False,
        auto_timestamps: Optional[bool] = False,
        created_at: Optional[AnyStr] = None,
        updated_at: Optional[AnyStr] = None,
    ):
        self._prepare_logger(log_level, debug)
        self.entity_properties = None
        self.driver = driver

        self.created_at = created_at if created_at is not None else 'created_at'
        self.updated_at = updated_at if updated_at is not None else 'updated_at'
        self.auto_timestamps = auto_timestamps

        self.entity = entity

    @property
    def entity(self) -> Type[Entity]:
        """Return base entity model"""
        return self._entity

    @entity.setter
    def entity(self, value: Type[Entity]):
        """Set property entity value"""

        if value is None:
            return

        instance = value()

        if not isinstance(instance, Entity):
            raise TypeError('Unexpected base model type, should be an instance of BaseModel.', )

        self._entity = value
        self.entity_properties = set(instance.to_dict(skip_none=False).keys())

    @abstractmethod
    def find_one(self, **kwargs) -> Any:
        """Find one record from passed filters."""
        raise NotImplementedError('find_one method is not implemented.')

    @abstractmethod
    def find_many(self, **kwargs) -> Union[None, List[Any]]:
        """Find many records from passed filters."""
        raise NotImplementedError('find_many method is not implemented.')

    @abstractmethod
    def insert_one(self, record: Entity) -> Any:
        """Insert one record to the DB and return the assigned ID"""
        raise NotImplementedError('insert_one method is not implemented.')

    @abstractmethod
    def insert_many(self, records: List[Entity]) -> Any:
        """Insert many records at once to the DB."""
        raise NotImplementedError('insert_many method is not implemented.')

    @abstractmethod
    def update(self, **kwargs) -> NoReturn:
        """Update records according parameters."""
        raise NotImplementedError('update method is not implemented.')

    @abstractmethod
    def delete(self, **kwargs) -> NoReturn:
        """Delete records according parameters."""
        raise NotImplementedError('delete method is not implemented.')

    @abstractmethod
    def _check_builder_requirements(self, operation: AnyStr) -> NoReturn:
        """Verify builder requirements to use base repository methods."""
        raise NotImplementedError('delete method is not implemented.')

    def _add_updated_at(self, data: Dict[AnyStr, Any]) -> Dict[AnyStr, Any]:
        """Add timestamp values if the auto_timestamps flag is configured.

        :param data: Current entity data as Dict object
        :return Dict[AnyStr, Any]: Modified entity data with timestamps
        """

        if not self.auto_timestamps:
            return data

        if self.updated_at:
            data[self.updated_at] = datetime.utcnow()

        return data

    def _add_created_at(self, data: Dict[AnyStr, Any]) -> Dict[AnyStr, Any]:
        """Add timestamp values if the auto_timestamps flag is configured.

        :param data: Current entity data as Dict object
        :return Dict[AnyStr, Any]: Modified entity data with timestamps
        """

        if not self.auto_timestamps:
            return data

        if self.created_at:
            data[self.created_at] = datetime.utcnow()

        return data

    def _prepare_logger(self, log_level: int, debug: bool) -> NoReturn:
        """Initialize internal logger.

        :param log_level: Logging level
        :param debug: Flag for debug mode
        """

        if log_level is None:
            log_level = logging.WARNING

        if debug:
            log_level = logging.DEBUG

        self.logger = logging.getLogger('pydbrepo')
        self.logger.setLevel(log_level)
