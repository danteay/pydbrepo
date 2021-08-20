"""Mongo repository implementation."""

from typing import Any, AnyStr, List, NoReturn, Optional, Type

from pydbrepo.drivers.qldb import QLDB
from pydbrepo.entity import Entity
from pydbrepo.errors import BuilderError
from pydbrepo.helpers import common, sql

from .repository import Repository


class MongoRepository(Repository):
    """No SQL AWS QLDB based repository

    :param driver: Database driver implementation
    :param table: Main table that will handle the repository
    :param entity: Class type that should be handled by the repository
    :param log_level: Logging level
    :param debug: Flag for debug mode
    :param auto_timestamps: Flag to insert timestamps on configured created_at and updated_at fields
    :param created_at: Name for created_at timestamp field
    :param updated_at: Name for updated_at timestamp field
    """

    def __init__(
        self,
        driver: QLDB,
        table: Optional[AnyStr] = None,
        entity: Optional[Type] = None,
        log_level: Optional[int] = None,
        debug: Optional[bool] = False,
        auto_timestamps: Optional[bool] = False,
        created_at: Optional[AnyStr] = None,
        updated_at: Optional[AnyStr] = None,
    ):
        super().__init__(driver, entity, log_level, debug, auto_timestamps, created_at, updated_at)
        self.__table = table

    def find_one(self, **kwargs) -> Any:
        pass

    def find_many(self, **kwargs) -> List[Any]:
        pass

    def insert_one(self, record: Entity) -> Any:
        pass

    def insert_many(self, records: List[Entity]) -> Any:
        pass

    def update(self, **kwargs) -> NoReturn:
        pass

    def delete(self, **kwargs) -> NoReturn:
        pass

    def _check_builder_requirements(self, operation: AnyStr) -> NoReturn:
        """Validate if there is a configured default table and base model to
        execute predefined query builder.

        :param operation: Operation name that is being evaluated
        :raise RepositoryBuilderError: If default table is None
        """

        if self.__table is None:
            raise BuilderError(
                f"Can't perform {operation} action without a default table. "
                "Please override the method.",
            )

        if self.entity is None:
            raise BuilderError(
                f"Can't perform {operation} action without a default base model. "
                "Please override the method.",
            )
