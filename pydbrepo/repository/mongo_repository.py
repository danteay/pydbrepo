"""Mongo repository implementation."""

from typing import Any, AnyStr, List, NoReturn, Optional, Type

from pydbrepo.drivers.mongo import Mongo, MongoAction, MongoActionType
from pydbrepo.entity import Entity
from pydbrepo.errors import BuilderError
from pydbrepo.helpers import common

from .repository import Repository


class MongoRepository(Repository):
    """No SQL Mongo based repository actions.

    :param driver: Database driver implementation
    :param collection: Main table that will handle the repository
    :param entity: Class type that should be handled by the repository
    :param log_level: Logging level
    :param debug: Flag for debug mode
    :param auto_timestamps: Flag to insert timestamps on configured created_at and updated_at fields
    :param created_at: Name for created_at timestamp field
    :param updated_at: Name for updated_at timestamp field
    """

    def __init__(
        self,
        driver: Mongo,
        collection: Optional[AnyStr] = None,
        entity: Optional[Type] = None,
        log_level: Optional[int] = None,
        debug: Optional[bool] = False,
        auto_timestamps: Optional[bool] = False,
        created_at: Optional[AnyStr] = None,
        updated_at: Optional[AnyStr] = None,
    ):
        super().__init__(driver, entity, log_level, debug, auto_timestamps, created_at, updated_at)
        self._collection = collection

    def find_one(self, **kwargs) -> Any:
        """Find one record from passed filters.

        :param kwargs: Parameters that will be process by the method.
            filters: Dict[AnyStr, Any] -> query filters that will be applied to Mongo builder
            order_by: Optional[MongoOrder] -> query ordering method
            order: Optional[int] -> Filed that should be ordered in query
            limit: Optional[int] -> Number or retrieved documents for the query
            offset: Optional[int] -> Number of omitted documents before the result

        :return Any: One record result
        """

        self._check_builder_requirements('find_one')

        params = {
            'filters': kwargs.get('filters'),
            'order_by': kwargs.get('order_by'),
            'order': kwargs.get('order'),
            'limit': kwargs.get('limit'),
            'offset': kwargs.get('offset')
        }

        record = self.driver.query_one(action=MongoAction.find, collection=self._collection, **params)

        if not record:
            return None

        return self.entity().from_dict(record)

    def find_many(self, **kwargs) -> Any:
        """Find one record from passed filters.

        :param kwargs: Parameters that will be process by the method.
            filters: Dict[AnyStr, Any] -> query filters that will be applied to Mongo builder
            order_by: Optional[MongoOrder] -> query ordering method
            order: Optional[int] -> Filed that should be ordered in query
            limit: Optional[int] -> Number or retrieved documents for the query
            offset: Optional[int] -> Number of omitted documents before the result

        :return Any: List of records found by query
        """

        self._check_builder_requirements('find_many')

        params = {
            'filters': kwargs['filters'],
            'order_by': kwargs.get('order_by', None),
            'order': kwargs.get('order', None),
            'limit': kwargs.get('limit', None),
            'offset': kwargs.get('offset', None)
        }

        records = self.driver.query(action=MongoAction.find, collection=self._collection, **params)

        if not records:
            return []

        return [self.entity().from_dict(record) for record in records]

    def insert_one(self, record: Entity, return_id: bool = False) -> Any:
        """Find one record from passed filters.

        :param record: Document to add to the collection
        :param return_id: Flag to return new inserted ids
        :return Any: List of records found by query
        """

        self._check_builder_requirements('insert_one')

        data = {key: common.handle_extra_types(value) for key, value in record.to_dict().items()}

        data = self._add_created_at(data)
        data = self._add_updated_at(data)

        result = self.driver.query_one(action=MongoAction.insert, collection=self._collection, data=data)

        if return_id:
            return result.inserted_id

        return None

    def insert_many(self, records: List[Entity], return_ids: bool = False) -> Any:
        """Find one record from passed filters.

        :param records: Documents to add to the collection
        :param return_ids: Flag to return new inserted ids
        :return Any: List of records found by query
        """

        self._check_builder_requirements('insert_many')

        data = []

        for record in records:
            record = {key: common.handle_extra_types(value) for key, value in record.to_dict().items()}

            record = self._add_created_at(record)
            record = self._add_updated_at(record)

            data.append(record)

        result = self.driver.query(action=MongoAction.insert, collection=self._collection, data=data)

        if return_ids:
            return result.inserted_ids

        return None

    def update(self, **kwargs) -> NoReturn:
        """Find one record from passed filters.

        :param kwargs: Properties for update query
            filters: Dict[AnyStr, Any] -> Filters to be applied on Mongo Query
            data: Union[Dict[AnyStr, Any], Entity] -> Data to be updated
        """

        self._check_builder_requirements('update')

        data = kwargs.get('data', None)

        if data is None:
            raise BuilderError("Can't update with empty data")

        if isinstance(data, Entity):
            data = data.to_dict()

        if not isinstance(data, dict):
            raise BuilderError("Data needs to be of type dict or an Entity object.")

        data = {key: common.handle_extra_types(value) for key, value in data.items()}
        data = self._add_updated_at(data)

        self.driver.query_none(
            action=MongoAction.update,
            collection=self._collection,
            filters=kwargs['filters'],
            type_=MongoActionType.many,
            data=data
        )

    def delete(self, **kwargs) -> NoReturn:
        """Delete records according filters

        :param kwargs: Properties for update query
            filters: Dict[AnyStr, Any] -> Filters to be applied on Mongo Query
        """

        self._check_builder_requirements('update')

        self.driver.query_none(
            action=MongoAction.delete,
            collection=self._collection,
            filters=kwargs['filters'],
            type_=MongoActionType.many,
        )

    def _check_builder_requirements(self, operation: AnyStr) -> NoReturn:
        """Validate if there is a configured default table and base model to
        execute predefined query builder.

        :param operation: Operation name that is being evaluated
        :raise RepositoryBuilderError: If default table is None
        """

        if self._collection is None:
            raise BuilderError(
                f"Can't perform {operation} action without a default table. Please override the method.",
            )

        if self.entity is None:
            raise BuilderError(
                f"Can't perform {operation} action without a default base model. Please override the method.",
            )
