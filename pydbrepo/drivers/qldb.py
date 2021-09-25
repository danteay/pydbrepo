"""Postgres driver."""

# pylint: disable=R0201

import os
from contextlib import ContextDecorator
from typing import Any, AnyStr, Dict, List, NoReturn, Optional, Tuple

from pyqldb.config.retry_config import RetryConfig
from pyqldb.driver.qldb_driver import QldbDriver
from pyqldb.execution.executor import Executor

from pydbrepo.drivers.driver import Driver
from pydbrepo.errors import DriverConfigError


class QLDBContext(ContextDecorator):
    """QLDB Query context implementation."""

    def __init__(self):
        self.result = []

    def __enter__(self):
        """start QLDB query context."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit from query context."""
        self.result = []


class QLDB(Driver):
    """QLDB connection Driver.

    Environment variable configs:
        DATABASE_NAME: ledger name
        QLDB_RETRY_CONF: integer defining number of retry attempts
        AWS_ACCESS_KEY_ID: User AWS access key
        AWS_SECRET_ACCESS_KEY: User AWS secret access key
        AWS_DEFAULT_REGION: AWS region where the ledger is hosted

    :type ledger: str
    :param ledger: The QLDB ledger name.

    :type retry: int
    :param retry: Config to specify max number of retries, base and custom backoff strategy for
        retries. Will be overridden if a different retry_config is passed to
        :py:meth:`pyqldb.driver.qldb_driver.QldbDriver.execute_lambda`.

    :type aws_access_key_id: str
    :param aws_access_key_id: AWS Access Key Id of the user that will be authenticated

    :type aws_secret_access_key: str
    :param aws_secret_access_key: AWS Secret Access Key of the user that will be authenticated

    :type aws_region: str
    :param aws_region: AWS Region code where the QLDB ledger is managed.
    """

    def __init__(
        self,
        ledger: Optional[AnyStr] = None,
        retry: Optional[int] = None,
        aws_access_key_id: Optional[AnyStr] = None,
        aws_secret_access_key: Optional[AnyStr] = None,
        aws_region: Optional[AnyStr] = None,
    ):
        super().__init__()
        self.__build_connection(ledger, retry, aws_access_key_id, aws_secret_access_key, aws_region)

    def query(self, **kwargs) -> List[Dict]:
        """Execute a query and return all values.

        :param kwargs: Parameters to execute query statement.
            sql: AnyStr -> SQL query statement
            args: Optional[Iterable[Any]] -> Object with query replacement values

        :return List[Tuple]: List of tuple records found by query
        """

        self._validate_params({'sql'}, set(kwargs.keys()))
        args = tuple(kwargs.get('args', []))

        with QLDBContext() as context:
            self.__conn.execute_lambda(
                lambda executor: self.__execute(executor, context, kwargs['sql'], args)
            )

            result = context.result

        if not result:
            return []

        return list(result)

    def query_one(self, **kwargs) -> Optional[Dict]:
        """Execute a query and return just the first result.

        :param kwargs: Parameters to execute query statement.
            sql: AnyStr -> SQL query statement
            args: Optional[Iterable[Any]] -> Object with query replacement values

        :return Tuple: Tuple record found by query
        """

        self._validate_params({'sql'}, set(kwargs.keys()))
        args = tuple(kwargs.get('args', []))

        with QLDBContext() as context:
            self.__conn.execute_lambda(
                lambda executor: self.__execute(executor, context, kwargs['sql'], args)
            )

            result = context.result

        if not result:
            return None

        return result[0]

    def query_none(self, **kwargs) -> NoReturn:
        """Execute a query and do not return any result value.

        :param kwargs: Parameters to execute query statement.
            sql: AnyStr -> SQL query statement
            args: Optional[Iterable[Any]] -> Object with query replacement values
        """

        self._validate_params({'sql'}, set(kwargs.keys()))
        args = tuple(kwargs.get('args', []))

        with QLDBContext() as context:
            self.__conn.execute_lambda(
                lambda executor: self.__execute(executor, context, kwargs['sql'], args)
            )

    @staticmethod
    def __execute(
        executor: Executor, context: QLDBContext, sql: AnyStr, args: Tuple[Any, ...]
    ) -> NoReturn:
        """Execute a query and store result in the shared context.

        :param executor: Transaction executor from driver connection
        :param context: QLDB Query context to store results
        :param sql: SQL query that will be executed
        :param args: Arguments that should be replaced on query
        """

        cursor = executor.execute_statement(sql, *args)
        context.result = list(map(dict, cursor))

    def commit(self) -> NoReturn:
        pass

    def rollback(self) -> NoReturn:
        pass

    def close(self) -> NoReturn:
        """Close connection."""

        self.__conn.close()

    def get_real_driver(self) -> QldbDriver:
        """Return real QLDB driver connection object.

        :return QldbDriver: Real connection
        """

        return self.__conn

    def placeholder(self, **kwargs) -> AnyStr:
        """Return place holder for SQL queries.

        :return AnyStr: Placeholder string token
        """

        return "?"

    def reset_placeholder(self) -> NoReturn:
        """No actions needed to reset place holder."""

    def __build_connection(
        self, ledger: AnyStr, retry: int, aws_access_key_id: AnyStr, aws_secret_access_key: AnyStr,
        aws_region: AnyStr
    ) -> NoReturn:
        """Build QLDB connection.

        :param ledger: Database ledger to connection
        :param retry: Number of retry attempts to connect with ledger
        :param aws_access_key_id: AWS access key ID of the user that will be connected
        :param aws_secret_access_key: AWS secret access key of the user that will connected
        :param aws_region: AWS Region where the ledger is hosted
        :raise DriverConfigError: When any needed param is missing
        """

        self.__params = self.__prepare_connection_params(
            ledger, retry, aws_access_key_id, aws_secret_access_key, aws_region
        )

        if self.__params['retry_config'] is not None:
            self.__params['retry_config'] = RetryConfig(
                retry_limit=int(self.__params['retry_config'])
            )
        else:
            self.__params['retry_config'] = RetryConfig(retry_limit=2)

        if None in self.__params.values():
            raise DriverConfigError(f'Missing configuration for QLDB driver: {self.__params}')

        self.__conn = QldbDriver(**self.__params)

    @staticmethod
    def __prepare_connection_params(
        ledger: AnyStr, retry: int, aws_access_key_id: AnyStr, aws_secret_access_key: AnyStr,
        aws_region: AnyStr
    ) -> Dict[AnyStr, Any]:

        params = {
            'ledger_name': ledger,
            'retry_config': retry,
            'region_name': aws_region,
            'aws_secret_access_key': aws_secret_access_key,
            'aws_access_key_id': aws_access_key_id
        }

        params = {key: value for key, value in params.items() if value is not None}

        envs = {
            'ledger_name': os.getenv("DATABASE_NAME", None),
            'retry_config': os.getenv("QLDB_RETRY_CONF", None),
            'region_name': os.getenv("AWS_DEFAULT_REGION", None),
            'aws_secret_access_key': os.getenv("AWS_SECRET_ACCESS_KEY", None),
            'aws_access_key_id': os.getenv("AWS_ACCESS_KEY_ID", None)
        }

        envs.update(params)

        return envs

    def __repr__(self):
        """QLDB driver representation."""
        return f"QLDB({str(self.__params)})"
