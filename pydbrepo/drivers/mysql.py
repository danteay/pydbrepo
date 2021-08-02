"""MySQL Driver implementation."""

# pylint: disable=R0201

import os
from typing import Any, AnyStr, Dict, List, NoReturn, Optional, Tuple, Union
from urllib.parse import urlparse

from mysql import connector

from pydbrepo.drivers.driver import Driver
from pydbrepo.errors import DriverConfigError


class Mysql(Driver):
    """Mysql Driver class.

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

    def _build_connection(
        self,
        url: Optional[AnyStr] = None,
        user: Optional[AnyStr] = None,
        pwd: Optional[AnyStr] = None,
        host: Optional[AnyStr] = None,
        port: Optional[AnyStr] = None,
        database: Optional[AnyStr] = None,
        autocommit: Optional[bool] = None,
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

        self._conn = connector.connect(**params)
        self._conn.autocommit = commit

    def _prepare_connection_parameters(
        self,
        url: Optional[AnyStr] = None,
        user: Optional[AnyStr] = None,
        pwd: Optional[AnyStr] = None,
        host: Optional[AnyStr] = None,
        port: Optional[AnyStr] = None,
        database: Optional[AnyStr] = None,
        autocommit: Optional[bool] = None,
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
            'user': os.getenv('DATABASE_USER', 'root'),
            'password': os.getenv('DATABASE_PASSWORD', None),
            'host': os.getenv('DATABASE_HOST', 'localhost'),
            'port': os.getenv('DATABASE_PORT', '3306'),
            'database': os.getenv('DATABASE_NAME', None),
            'autocommit': os.getenv('DATABASE_COMMIT', 'false').lower() == 'true'
        }

        envs.update(params)
        envs.update(self._parse_url_connection(envs['url']))
        del envs['url']

        return envs

    @staticmethod
    def _parse_url_connection(url: AnyStr) -> Dict[AnyStr, Any]:
        """Parse an standard URL and return his params.

        :param url: Standard DB connection url
        :return Dict[AnyStr, Any]: Connection params
        """

        if url is None:
            return {}

        parsed = urlparse(url)

        if parsed.scheme != 'mysql':
            raise DriverConfigError('Invalid database URL scheme.')

        data = {
            'user': parsed.username,
            'password': parsed.password,
            'host': parsed.hostname,
            'port': parsed.port,
        }

        if parsed.path:
            data['database'] = parsed.path[1:]

        return data

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

    def query(self, **kwargs) -> List[Tuple]:
        """Execute a query and return all values.

        :param kwargs: Parameters to execute query statement.
            sql: AnyStr -> SQL query statement
            args: Optional[Iterable[Any]] -> Object with query replacement values

        :return List[Tuple]: List of tuple records found by query
        """

        self._validate_params({'sql'}, set(kwargs.keys()))
        cursor = self._conn.cursor(buffered=True)

        _ = self._execute(cursor, kwargs['sql'], *kwargs.get('args', []))
        res = cursor.fetchall()

        cursor.close()

        return res

    def query_one(self, **kwargs) -> Any:
        """Execute a query and do not return any result value.

        :param kwargs: Parameters to execute query statement.
            sql: AnyStr -> SQL query statement
            args: Optional[Iterable[Any]] -> Object with query replacement values

        :return Tuple: Found record
        """

        self._validate_params({'sql'}, set(kwargs.keys()))
        cursor = self._conn.cursor(buffered=True)

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
        """Commit transaction."""
        self._conn.commit()

    def rollback(self) -> NoReturn:
        self._conn.rollback()

    def close(self) -> NoReturn:
        """Close current connection."""
        self._conn.close()

    def get_real_driver(self) -> Any:
        """Return real mysql driver connection."""
        return self._conn

    def placeholder(self, **kwargs) -> AnyStr:
        """Return query place holder."""
        return '%s'

    def reset_placeholder(self) -> NoReturn:
        """Reset place holder status (do nothing)"""

    def __repr__(self):
        """Mysql driver representation."""
        return f"Mysql({str(self._params)})"
