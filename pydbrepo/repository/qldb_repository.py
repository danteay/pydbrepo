"""Mongo repository implementation."""

from typing import Any, AnyStr, Dict, List, NoReturn, Optional, Type

from pypika import Query

from pydbrepo.drivers.qldb import QLDB
from pydbrepo.entity import Entity
from pydbrepo.helpers import common, sql

from ..errors import BuilderError
from .repository import Repository


class QldbRepository(Repository):
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
        """Find one record from passed filters.

        :param kwargs: Parameters that will be process by the method.
            select: Iterable[AnyStr] -> List of fields that will be selected by the query.
            Any other field wil be treat as a field filter like `id = 12` or `name = 'Jhon Dou'`.

        :return Entity: Configured entity instance with record information
        """

        common.check_builder_requirements('find_one', self.__table, self.entity)
        fields = sql.prepare_selected_fields(kwargs.get('select', None), self.entity_properties)

        params = []
        sql_query = Query.from_(self.__table).select(*fields)

        sql_query, where_values = sql.add_where_statements(
            query=sql_query,
            data=kwargs,
            entity_properties=self.entity_properties,
            skip={'select'},
            driver=self.driver
        )

        params.extend(where_values)

        self.logger.debug(f"SQL: {str(sql_query)}")

        record = self.driver.query_one(sql=str(sql_query), args=params)

        if not record:
            return None

        return self.entity().from_dict(record)

    def find_many(self, **kwargs) -> List[Any]:
        """Find one record from passed filters. Note that QLDB do not support limit or offset
        statements.

        :param kwargs: Parameters that will be process by the method.
            select: Optional[Iterable[AnyStr]] -> List of fields that will be selected by the query
            order_by: Optional[List[Tuple[AnyStr, pypika.Order]]] -> Ordering method
            group_by: Optional[List[AnyStr]] -> Groping fields

            Any other field wil be treat as a field filter like `id=12` or `name='Jhon Dou'`.

        :return List[Any]: List of collected records by the corresponding filters
        """

        common.check_builder_requirements('find_many', self.__table, self.entity)
        fields = sql.prepare_selected_fields(kwargs.get('select', None), self.entity_properties)

        params = []
        sql_query = Query.from_(self.__table).select(*fields)

        sql_query, where_values = sql.add_where_statements(
            query=sql_query,
            data=kwargs,
            entity_properties=self.entity_properties,
            skip={'select', 'order_by', 'group_by'},
            driver=self.driver
        )

        params.extend(where_values)

        sql_query = sql.add_order_by(sql_query, kwargs)
        sql_query = sql.add_group_by(sql_query, kwargs)

        self.logger.debug(f"SQL: {str(sql_query)}")

        records = self.driver.query(sql=str(sql_query), args=params)

        if not records:
            return []

        return [self.entity().from_dict(record) for record in records]

    def insert_one(self, record: Entity) -> NoReturn:
        """Insert one record from an entity instance.

        :param record: New record data.

        :raise BuilderError: When record is empty

        :return NoReturn: Returning requested fields
        """

        if not record:
            raise BuilderError("Can't insert an empty record.")

        common.check_builder_requirements('insert_one', self.__table, self.entity)

        data = self.__build_records([record])[0]
        sql_query = f"INSERT INTO {self.__table} {self.driver.placeholder()}"

        self.logger.debug(f"SQL: {sql_query}")

        self.driver.query_none(sql=str(sql_query), args=(data, ))

    def insert_many(self, records: List[Entity]) -> NoReturn:
        """Insert many records at once from entity objects.

        :param records: List of new records with data.

        :raise BuilderError: When records are empty.
        """

        if not records:
            raise BuilderError("Can't insert an empty record.")

        common.check_builder_requirements('insert_one', self.__table, self.entity)

        data = self.__build_records(records)
        sql_query = f"INSERT INTO {self.__table} {self.driver.placeholder()}"

        self.logger.debug(f"SQL: {sql_query}")

        self.driver.query_none(sql=str(sql_query), args=(data, ))

    def update(self, data: Dict[AnyStr, Any], **kwargs) -> NoReturn:
        """Update some records with new data according filters.

        :param data: New data to be updated.
        :param kwargs: Filter members like `id=12` or `email='some@mail.com'`
        """

        common.check_builder_requirements('update', self.__table, self.entity)

        data = self._add_updated_at(data)

        values = []
        sql_query = Query.update(self.__table)

        sql_query, set_values = sql.add_set_statements(
            query=sql_query,
            data=data,
            entity_properties=self.entity_properties,
            driver=self.driver
        )

        values.extend(set_values)

        sql_query, where_values = sql.add_where_statements(
            query=sql_query,
            data=kwargs,
            entity_properties=self.entity_properties,
            driver=self.driver
        )

        values.extend(where_values)

        self.logger.debug(f"SQL: {str(sql_query)}")
        self.driver.query_none(sql=str(sql_query), args=values)

    def delete(self, **kwargs) -> NoReturn:
        """Execute a DELETE query over the configured table entity.

        :param kwargs: Filter parameters for the query statement
        """

        common.check_builder_requirements('delete', self.__table, self.entity)

        sql_query = Query.from_(self.__table).delete()
        sql_query, values = sql.add_where_statements(
            query=sql_query,
            data=kwargs,
            entity_properties=self.entity_properties,
            driver=self.driver
        )

        self.logger.debug(f"SQL: {str(sql_query)}")
        self.driver.query_none(sql=str(sql_query), args=values)

    def __build_records(self, records: List[Entity]) -> List[Dict[AnyStr, Any]]:
        """Normalize types and add needed fields of the records after being operated.

        :param records: Denormalized records

        :return List[Dict[AnyStr, Any]]: Normalized records
        """

        final = []

        for record in records:
            data = {
                key: common.handle_extra_types(value)
                for key, value in record.to_dict().items()
            }

            data = self._add_created_at(data)
            data = self._add_updated_at(data)

            final.append(data)

        return final
