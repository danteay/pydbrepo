"""SQLite Driver implementation."""

# pylint: disable=R0201

import os
import sqlite3
from typing import Any, AnyStr, List, NoReturn, Optional, Tuple

from pydbrepo.drivers.driver import Driver


class SQLite(Driver):
    """SQLite Driver connection class.

    Environment variables:
        DATABASE_URL: Database file ulr on the system. If it's an in memory database the url should
            be None or `:memory:` string

        DATABASE_COMMIT: default('false') Auto commit transaction flag

    :type url:
    :param url: Database connection url
    :param autocommit: Auto commit transactions
    """

    def __init__(
        self,
        url: Optional[AnyStr] = None,
        autocommit: Optional[bool] = None,
    ):
        super().__init__()
        self.__build_connection(url, autocommit)

    def __build_connection(
        self,
        url: Optional[AnyStr] = None,
        autocommit: Optional[bool] = None,
    ) -> NoReturn:
        """Start real driver connection from parameters.

        :param url: Database connection url
        :param autocommit: Auto commit transactions
        """

        if url is None:
            url = ':memory:'

        if autocommit is None:
            autocommit = False

        if os.getenv('DATABASE_URL', None) is not None:
            url = os.getenv('DATABASE_URL')

        if os.getenv('DATABASE_COMMIT', None) is not None:
            autocommit = os.getenv('DATABASE_COMMIT').lower() == "true"

        self.__url = url
        self.__conn = sqlite3.connect(url)
        self.__commit = autocommit

    @staticmethod
    def __execute(cursor, sql: AnyStr, *args) -> Any:
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
        cursor = self.__conn.cursor()

        _ = self.__execute(cursor, kwargs['sql'], *kwargs.get('args', []))
        self.__commit_transaction()
        res = cursor.fetchall()

        cursor.close()

        return res

    def query_one(self, **kwargs) -> Tuple[Any, ...]:
        """Execute a query and do not return any result value.

        :param kwargs: Parameters to execute query statement.
            sql: AnyStr -> SQL query statement
            args: Optional[Iterable[Any]] -> Object with query replacement values

        :return Tuple: Found record
        """

        self._validate_params({'sql'}, set(kwargs.keys()))
        cursor = self.__conn.cursor()

        _ = self.__execute(cursor, kwargs['sql'], *kwargs.get('args', []))
        self.__commit_transaction()
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
        cursor = self.__conn.cursor()

        _ = self.__execute(cursor, kwargs['sql'], *kwargs.get('args', []))
        self.__commit_transaction()

        cursor.close()

    def commit(self) -> NoReturn:
        """Commit transaction."""
        self.__conn.commit()

    def rollback(self) -> NoReturn:
        self.__conn.rollback()

    def close(self) -> NoReturn:
        """Close current connection."""
        self.__conn.close()

    def get_real_driver(self) -> Any:
        """Return real mysql driver connection."""
        return self.__conn

    def placeholder(self, **kwargs) -> AnyStr:
        """Return query place holder."""
        return '?'

    def reset_placeholder(self) -> NoReturn:
        """Reset place holder status (do nothing)"""

    def __repr__(self):
        """Mysql driver representation."""
        return f"SQLite({self.__url})"

    def __commit_transaction(self):
        """Execute commit operation if the __commit flag is True."""

        if self.__commit:
            self.commit()
