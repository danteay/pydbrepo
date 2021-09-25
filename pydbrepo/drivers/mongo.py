"""Mongo driver."""

# pylint: disable=R0201,C0103

import os
import ssl
from enum import Enum
from typing import Any, AnyStr, Dict, NoReturn, Optional, Set, Union

import pymongo
from pymongo.collection import Cursor
from pymongo.results import (DeleteResult, InsertManyResult, InsertOneResult, UpdateResult)

from pydbrepo.drivers.driver import Driver
from pydbrepo.errors import (BuilderError, DriverConfigError, DriverExecutionError, QueryError)
from pydbrepo.helpers import mongo


class MongoAction(str, Enum):
    """Types of actions than Mongo Driver can perform."""

    find = 'find'
    insert = 'insert'
    update = 'update'
    delete = 'delete'


class MongoActionType(str, Enum):
    """Define action variations of the same operation"""

    one = 'one'
    many = 'many'
    none = 'none'


class MongoOrder(Enum):
    """Define find query ordering."""

    asc = pymongo.ASCENDING
    desc = pymongo.DESCENDING


class Mongo(Driver):
    """Driver implementation for MongoDB.

    Environment variables:
        DATABASE_URL: [1]
        DATABASE_USER: Database username
        DATABASE_PASSWORD: Database user password
        DATABASE_HOST: default('localhost') Database host
        DATABASE_PORT: default('27017') Database port
        DATABASE_NAME: Database name

    :type url: str
    :param url: Database connection url with standard format [1]

    :type user: str
    :param user: Database user name

    :type pwd: str
    :param pwd: Database user password

    :type host: str
    :param host: Database host

    :type port: str
    :param port: Database port number

    :type database: str
    :param database: Database name

    :type kwargs: named variadic
    :param kwargs: Any other pymongo.MongoClient configuration

    [1] Standard URL format:
        mongodb[+srv]://<username>:<password>@<host>:<port>/<database>[?<arguments>]
    """

    def __init__(
        self,
        url: Optional[AnyStr] = None,
        user: Optional[AnyStr] = None,
        pwd: Optional[AnyStr] = None,
        host: Optional[AnyStr] = None,
        port: Optional[AnyStr] = None,
        database: Optional[AnyStr] = None,
        **kwargs,
    ):
        super().__init__()
        self.__database = None
        self.__build_connection(url, user, pwd, host, port, database, **kwargs)

    @property
    def database(self) -> AnyStr:
        """return stored database mane"""
        return self.__database

    @database.setter
    def database(self, value: AnyStr):
        """Set nu database name for use in connection"""
        self.__database = value

    def ping(self):
        """Check database Connection.

        :raise DriverConfigError: In case of Mongo ping command fails
        """

        res = self.__conn[self.__database].command("ping")

        if res["ok"] != 1.0:
            raise DriverConfigError("Database connection error")

    @staticmethod
    def __prepare_client_extra_params(**kwargs) -> Dict[AnyStr, Any]:
        """Verify if there are specific configurations for MongoClient, if not add some basic
        config.

        :param kwargs: Client extra configuration
        :return Dict[AnyStr, Any]: Updated configuration
        """

        client_extra_params = set(kwargs.keys())

        if 'ssl' not in client_extra_params:
            kwargs['ssl'] = True

        if 'ssl_cert_reqs' not in client_extra_params:
            kwargs['ssl_cert_reqs'] = ssl.CERT_NONE

        return kwargs

    @staticmethod
    def __prepare_connection_parameters(
        url: AnyStr = None,
        user: AnyStr = None,
        pwd: AnyStr = None,
        host: AnyStr = None,
        port: AnyStr = None,
        database: AnyStr = None,
    ) -> Dict[AnyStr, AnyStr]:
        """Validate connection parameters an try to fill it from env vars if they are not set.

        :param url: Database connection url
        :param user: Database user name
        :param pwd: Database user password
        :param host: Database host
        :param port: Database port number
        :param database: Database name

        :return Dict[AnyStr, AnyStr]: Connection parameters

        :raise DriverConfigError: If connection url and connection user are None at the same time
        """

        params = {
            'url': url,
            'user': user,
            'password': pwd,
            'host': host,
            'port': port,
            'database': database,
        }

        params = {key: value for key, value in params.items() if value is not None}

        envs = {
            'url': os.getenv('DATABASE_URL', None),
            'user': os.getenv('DATABASE_USER', None),
            'password': os.getenv('DATABASE_PASSWORD', None),
            'host': os.getenv('DATABASE_HOST', 'localhost'),
            'port': os.getenv('DATABASE_PORT', '27017'),
            'database': os.getenv('DATABASE_NAME', None),
        }

        envs.update(params)

        if envs['url'] is not None:
            envs['host'] = None
            envs['port'] = None

        if envs['url'] is None and envs['user'] is None:
            raise DriverConfigError('Invalid connection params. Not user detected.')

        return envs

    def query(self, **kwargs) -> Any:
        """Execute query over a specific collection and return multiple values.

        :param kwargs: Definition of the query execution.
            action: MongoAction -> (find, insert, update, delete)
            collection: AnyStr -> Name of the queried collection
            filters: Optional[Dict[AnyStr, Any]] -> pymongo query filters that should be applied
            order_by: Optional[MongoOrder] -> query ordering method
            order: Optional[int] -> Filed that should be ordered in query
            limit: Optional[int] -> Number or retrieved documents for the query
            offset: Optional[int] -> Number of omitted documents before the result


        :return Any: List of found elements
        """

        self.__validate_kwargs(**kwargs)

        if 'type_' in set(kwargs.keys()):
            del kwargs['type_']

        return self._execute_method(type_=MongoActionType.many, **kwargs)

    def query_one(self, **kwargs) -> Any:
        """Execute query over a specific collection and return multiple values.

        :param kwargs: Definition of the query execution.
            action: MongoAction -> (find, insert, update, delete)
            collection: AnyStr -> Name of the queried collection
            filters: Dict[AnyStr, Any] -> pymongo query filters that should be applied
            order_by: Optional[MongoOrder] -> query ordering method
            order: Optional[int] -> Filed that should be ordered in query
            limit: Optional[int] -> Number or retrieved documents for the query
            offset: Optional[int] -> Number of omitted documents before the result

        :return Any: List of found elements
        """

        self.__validate_kwargs(**kwargs)

        if 'type_' in set(kwargs.keys()):
            del kwargs['type_']

        return self._execute_method(type_=MongoActionType.one, **kwargs)

    def query_none(self, **kwargs) -> NoReturn:
        """Execute query over a specific collection and do not return any record.

        :param kwargs: Definition of the query execution.
            action: MongoAction -> (find, insert, update, delete)
            collection: AnyStr -> Name of the queried collection
            filters: Dict[AnyStr, Any] -> pymongo query filters that should be applied
            order_by: Optional[MongoOrder] -> query ordering method
            order: Optional[int] -> Filed that should be ordered in query
            limit: Optional[int] -> Number or retrieved documents for the query
            offset: Optional[int] -> Number of omitted documents before the result
        """

        self.__validate_kwargs(**kwargs)

        if 'type_' in set(kwargs.keys()):
            del kwargs['type_']

        return self._execute_method(type_=MongoActionType.none, **kwargs)

    def commit(self) -> NoReturn:
        """Commit transaction."""
        raise NotImplementedError('Method is not implemented')

    def rollback(self) -> NoReturn:
        """Rollback transaction."""
        raise NotImplementedError('Method is not implemented')

    def close(self) -> NoReturn:
        """Close current connection."""
        self.__conn.close()

    def get_real_driver(self) -> Any:
        """Return real driver connection"""
        return self.__conn

    def placeholder(self, **kwargs) -> AnyStr:
        """Query placeholder not needed on Mongo queries"""
        raise NotImplementedError(
            'Method is not implemented because is not needed for Mongo queries'
        )

    def reset_placeholder(self) -> NoReturn:
        """Reset placeholder not needed on Mongo queries."""
        raise NotImplementedError(
            'Method is not implemented because is not needed for Mongo queries'
        )

    def _execute_method(
        self,
        action: MongoAction,
        type_: MongoActionType,
        collection: AnyStr,
        filters: Optional[Dict[AnyStr, Any]] = None,
        data: Optional[Dict[AnyStr, Any]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: Optional[AnyStr] = None,
        order: Optional[MongoOrder] = None
    ) -> Any:
        """Return the execution function according mongo action.

        :param action: Name of the action to perform
        :param type_: Variation type of the mongo operation
        :param collection: Name of the database collection where will be performed the action
        :param filters: Filters to apply on Mongo query
        :param data: Data to be used on Mongo query
        :param order_by: Filed that should be ordered in query
        :param order: Query ordering method
        :param limit: Number or retrieved documents for the query
        :param offset: Number of omitted documents before the result

        :return Any: Result of query execution

        :raise DriverExecutionError: When invalid mongo operation is defined
        """

        if action == MongoAction.find:
            return self.__find(type_, collection, filters, limit, offset, order_by, order)

        if action == MongoAction.insert:
            return self.__insert(type_, collection, data)

        if action == MongoAction.update:
            return self.__update(type_, collection, filters, data)

        if action == MongoAction.delete:
            return self.__delete(type_, collection, filters)

        raise DriverExecutionError(f'Invalid {action} operation was called')

    def __find(
        self,
        type_: MongoActionType,
        collection: AnyStr,
        filters: Dict[AnyStr, Any],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: Optional[AnyStr] = None,
        order: Optional[MongoOrder] = None
    ) -> Cursor:
        """Return find method variation of MongoClient connection.

        :param type_: Variation type of the mongo operation
        :param collection: Name of the database collection where will be performed the action
        :param filters: Query filters for query execution
        :param order_by: Query ordering field
        :param order: Query ordering method
        :param limit: Number or retrieved documents for the query
        :param offset: Number of omitted documents before the result

        :return Cursor: Raw Driver result

        :raise DriverExecutionError: When the query variation is not supported
        """

        if type_ == MongoActionType.one:
            return self.__conn[self.__database][collection].find_one(filters)

        if type_ == MongoActionType.many:
            find = self.__conn[self.__database][collection].find(filters)
            find = mongo.add_limit(find, limit)
            find = mongo.add_offset(find, offset)
            find = mongo.add_order_by(find, order_by, order)
            return find

        raise DriverExecutionError(f'Invalid variation {type_} of find method')

    def __insert(self, type_: MongoActionType, collection: AnyStr,
                 data: Dict[AnyStr, Any]) -> Union[InsertOneResult, InsertManyResult]:
        """Return insert method variation of MongoClient connection.

        :param type_: Variation type of the mongo operation
        :param collection: Name of the database collection where will be performed the action
        :param data: New data to be inserted

        :return Union[InsertOneResult, InsertManyResult]: Raw result of driver operation

        :raise BuilderError: When insert action is called with empty data
        :raise DriverExecutionError: When the query variation is not supported
        """

        if type_ == MongoActionType.one:
            if data is None:
                raise BuilderError("Can't insert empty data")

            return self.__conn[self.__database][collection].insert_one(data)

        if type_ in {MongoActionType.none, MongoActionType.many}:
            if len(data) < 1:
                raise BuilderError("Can't insert empty data")

            return self.__conn[self.__database][collection].insert_many(data)

        raise DriverExecutionError(f'Invalid variation {type_} of insert method')

    def __update(
        self,
        type_: MongoActionType,
        collection: AnyStr,
        filters: Dict[AnyStr, Any],
        data: Dict[AnyStr, Any],
    ) -> UpdateResult:
        """Return update method variation of MongoClient connection.

        :param type_: Variation type of the mongo operation
        :param collection: Name of the database collection where will be performed the action
        :param filters: Filters to apply on Mongo query
        :param data: Data to be used by Mongo query

        :return UpdateResult: Raw result of driver operation

        :raise BuilderError: When update action is called with empty data
        :raise DriverExecutionError: When the query variation is not supported
        """

        if type_ == MongoActionType.one:
            if data is None:
                raise BuilderError("Can't update empty data")

            return self.__conn[self.__database][collection].update_one(filters, {"$set": data})

        if type_ in {MongoActionType.none, MongoActionType.many}:
            if len(data) < 1:
                raise BuilderError("Can't update empty data")

            return self.__conn[self.__database][collection].update_many(filters, {"$set": data})

        raise DriverExecutionError(f'Invalid variation {type_} of update method')

    def __delete(
        self, type_: MongoActionType, collection: AnyStr, filters: Dict[AnyStr, Any]
    ) -> DeleteResult:
        """Return delete method variation of MongoClient connection.

        :param type_: Variation type of the mongo operation
        :param collection: Name of the database collection where will be performed the action
        :param filters: Filters to apply on Mongo query
        :return Callable: function to execute operation
        :raise DriverExecutionError: When the query variation is not supported
        """

        if type_ == MongoActionType.one:
            return self.__conn[self.__database][collection].delete_one(filters)

        if type_ in {MongoActionType.none, MongoActionType.many}:
            return self.__conn[self.__database][collection].delete_many(filters)

        raise DriverExecutionError(f'Invalid variation {type_} of delete method')

    def __validate_kwargs(self, **kwargs) -> NoReturn:
        """Validation for query kwargs and check if there are the necessary options.

        :param kwargs: All possible configurations of a Mongo query
        :raise QueryError: If filters is not present in actions different of insert
        """

        keys = set(kwargs.keys())

        self._validate_params({'action', 'collection'}, keys)
        self.__validate_filter(kwargs['action'], keys)

    @staticmethod
    def __validate_filter(action: MongoAction, keys: Set[AnyStr]) -> NoReturn:
        """Validate if the current action needs filters mandatory.

        :param action: Current query action
        :param keys: Current configuration names for the query
        :raise QueryError: If query action needs filters and this are not configured
        """

        if action != MongoAction.insert and 'filters' not in keys:
            raise QueryError(f'Action {action} needs filters to be executed')

    def __build_connection(
        self,
        url: Optional[AnyStr] = None,
        user: Optional[AnyStr] = None,
        pwd: Optional[AnyStr] = None,
        host: Optional[AnyStr] = None,
        port: Optional[AnyStr] = None,
        database: Optional[AnyStr] = None,
        **kwargs,
    ) -> NoReturn:
        """start real driver connection from parameters.

        :param url: Database connection url
        :param user: Database user name
        :param pwd: Database user password
        :param host: Database host
        :param port: Database port number
        :param database: Database name
        :param kwargs: Any other pymongo.MongoClient configuration
        """

        self.__params = self.__prepare_connection_parameters(url, user, pwd, host, port, database)
        kwargs = self.__prepare_client_extra_params(**kwargs)
        params = self.__params

        self.__database = params['database']
        del params['database']

        if params['url'] is not None:
            self.__conn = pymongo.MongoClient(params['url'], **kwargs)
            return

        self.__conn = pymongo.MongoClient(
            host=params['host'],
            port=int(params['port']),
            username=params['user'],
            password=params['pwd'],
            **kwargs
        )

    def __repr__(self):
        """Mongo driver representation."""
        return f"Mongo({str(self.__params)})"
