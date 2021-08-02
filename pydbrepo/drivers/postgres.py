"""Postgres driver."""

# pylint: disable=R0201

import os
from typing import Any, AnyStr, Dict, List, NoReturn, Optional, Tuple, Union

import psycopg2

from pydbrepo.drivers.driver import Driver
from pydbrepo.errors import DriverConfigError

__all__ = ['Postgres']


class Postgres(Driver):
    """Postgres connection manager.

    :param url: Database connection url
    :param user: Database user name
    :param pwd: Database user password
    :param host: Database host
    :param port: Database port number
    :param database: Database name
    :param autocommit: Auto commit transactions
    """

    def __init__(
        self,
        url: Optional[AnyStr] = None,
        user: Optional[AnyStr] = None,
        pwd: Optional[AnyStr] = None,
        host: Optional[AnyStr] = None,
        port: Optional[AnyStr] = None,
        database: Optional[AnyStr] = None,
        autocommit: Optional[bool] = None,
    ):
        super().__init__()
        self._build_connection(url, user, pwd, host, port, database, autocommit)

    def query(self, **kwargs) -> List[Tuple]:
        """Execute a query and return all values.

        :param kwargs: Parameters to execute query statement.
            sql: AnyStr -> SQL query statement
            args: Optional[Iterable[Any]] -> Object with query replacement values

        :return List[Tuple]: List of tuple records found by query
        """

        self._validate_params({'sql'}, set(kwargs.keys()))

        cursor = self._conn.cursor()

        _ = self._execute(cursor, kwargs['sql'], *kwargs.get('args', []))
        res = cursor.fetchall()

        cursor.close()

        return res

    def query_one(self, **kwargs) -> Tuple:
        """Execute a query and return just the first result.

        :param kwargs: Parameters to execute query statement.
            sql: AnyStr -> SQL query statement
            args: Optional[Iterable[Any]] -> Object with query replacement values

        :return Tuple: Tuple record found by query
        """

        self._validate_params({'sql'}, set(kwargs.keys()))

        cursor = self._conn.cursor()

        _ = self._execute(cursor, kwargs['sql'], *kwargs.get('args', []))
        res = cursor.fetchone()

        cursor.close()

        return res

    def query_none(self, **kwargs) -> NoReturn:
        """Execute a query and do not return any result value.

        :param kwargs: Parameters to execute query statement.
            sql: AnyStr -> SQL query statement
            args: Optional[Iterable[Any]] -> Object with query replacement values
        """

        self._validate_params({'sql'}, set(kwargs.keys()))

        cursor = self._conn.cursor()

        _ = self._execute(cursor, kwargs['sql'], *kwargs.get('args', []))
        cursor.close()

    def commit(self) -> NoReturn:
        """Commit transaction in DB."""
        self._conn.commit()

    def rollback(self) -> NoReturn:
        """Rollback transaction."""
        self._conn.rollback()

    def close(self) -> NoReturn:
        """Close database connection."""
        self._conn.close()

    def get_real_driver(self) -> Any:
        """Get real driver connection instance."""
        return self._conn

    def placeholder(self, **kwargs) -> AnyStr:
        """Return the next place holder param for prepared statements.

        :return AnyStr: Placeholder token
        """

        return '%s'

    def reset_placeholder(self) -> NoReturn:
        """Reset place holder status (do nothing)"""

    @staticmethod
    def _execute(cursor, sql: AnyStr, *args):
        """Execute query and attempt to replace with arguments.

        :param cursor: Connection cursor statement
        :param sql: Raw query to be executed
        :param args: List of arguments passed to be replaced in query
        """

        if not args:
            return cursor.execute(sql)

        return cursor.execute(sql, tuple(args))

    def _build_connection(
        self,
        url: AnyStr = None,
        user: AnyStr = None,
        pwd: AnyStr = None,
        host: AnyStr = None,
        port: AnyStr = None,
        database: AnyStr = None,
        autocommit: bool = None,
    ) -> NoReturn:
        """start real driver connection from parameters.

        :param url: Database connection url
        :param user: Database user name
        :param pwd: Database user password
        :param host: Database host
        :param port: Database port number
        :param database: Database name
        :param autocommit: Auto commit transactions
        """

        self._params = self._prepare_connection_parameters(url, user, pwd, host, port, database, autocommit)
        params = self._params

        commit = params['autocommit']
        del params['autocommit']

        if params['url'] is not None:
            self._conn = psycopg2.connect(params['url'])
            self._conn.autocommit = commit
            return

        del params['url']

        self._conn = psycopg2.connect(**params)
        self._conn.autocommit = commit

    @staticmethod
    def _prepare_connection_parameters(
        url: AnyStr = None,
        user: AnyStr = None,
        pwd: AnyStr = None,
        host: AnyStr = None,
        port: AnyStr = None,
        database: AnyStr = None,
        autocommit: bool = None,
    ) -> Dict[AnyStr, Union[AnyStr, bool]]:
        """Validate connection parameters an try to fill it from env vars if they are not set.

        :param url: Database connection url
        :param user: Database user name
        :param pwd: Database user password
        :param host: Database host
        :param port: Database port number
        :param database: Database name
        :param autocommit: Auto commit transactions
        :return Dict[AnyStr, Union[AnyStr, bool]]: Connection parameters
        :raise DriverConfigError: If connection url and connection user are None at the same time
        """

        params = {
            'url': url,
            'user': user,
            'password': pwd,
            'host': host,
            'port': port,
            'database': database,
            'autocommit': autocommit
        }

        params = {key: value for key, value in params.items() if value is not None}

        envs = {
            'url': os.getenv('DATABASE_URL', None),
            'user': os.getenv('DATABASE_USER', None),
            'password': os.getenv('DATABASE_PASSWORD', None),
            'host': os.getenv('DATABASE_HOST', 'localhost'),
            'port': os.getenv('DATABASE_PORT', '5432'),
            'database': os.getenv('DATABASE_NAME', 'postgres'),
            'autocommit': os.getenv('DATABASE_COMMIT', 'false').lower() == 'true'
        }

        envs.update(params)

        if envs['url'] is not None:
            envs['host'] = None
            envs['port'] = None
            envs['database'] = None

        if envs['url'] is None and envs['user'] is None:
            raise DriverConfigError('Invalid connection params. Not user detected.')

        return envs

    def __repr__(self):
        """Postgres driver representation."""
        return f"Postgres({str(self._params)})"
