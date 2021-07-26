"""Abstraction for SQL based drivers."""

from abc import ABC, abstractmethod
from contextlib import ContextDecorator
from typing import Any, AnyStr, NoReturn, Set

from pydbrepo.errors import QueryError

__all__ = ['Driver']


class Driver(ABC, ContextDecorator):
    """Abstract Driver definition."""

    def __enter__(self):
        """Enter as context class."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit as context context class."""
        self.close()

    @abstractmethod
    def query(self, **kwargs) -> Any:
        """Execute a query that returns many records"""
        raise NotImplementedError('query method is not implemented')

    @abstractmethod
    def query_one(self, **kwargs) -> Any:
        """Execute a query that return just one record"""
        raise NotImplementedError('query_one method is not implemented')

    @abstractmethod
    def query_none(self, **kwargs) -> NoReturn:
        """Execute a query that doesn't return any record"""
        raise NotImplementedError('query_none method is not implemented')

    @abstractmethod
    def commit(self) -> NoReturn:
        """Commit transaction on DB to persist operations."""
        raise NotImplementedError('commit method is not implemented')

    @abstractmethod
    def rollback(self) -> NoReturn:
        """rollback failure operation."""
        raise NotImplementedError('rollback method is not implemented')

    @abstractmethod
    def close(self) -> NoReturn:
        """Close current connection."""
        raise NotImplementedError('close method is not implemented')

    @abstractmethod
    def get_real_driver(self) -> Any:
        """Return the current real driver instance."""
        raise NotImplementedError('get_real_driver method is not implemented')

    @abstractmethod
    def placeholder(self, **kwargs) -> AnyStr:
        """Return the next driver placeholder for prepared statements"""
        raise NotImplementedError('placeholder method is not implemented')

    @abstractmethod
    def reset_placeholder(self) -> NoReturn:
        """This method is used to reset numeric based placeholders."""
        raise NotImplementedError('reset_placeholder method is not implemented')

    @staticmethod
    def _validate_params(needed: Set[AnyStr], params: Set[AnyStr]):
        """Validate if the needed params are present in kwargs of a method.

        :param needed: List of needed parameters
        :param params: Current function params
        :raise QueryError: If any of the needed params is not set
        """

        length = len(params)
        params.update(needed)

        if len(params) > length:
            raise QueryError(f'Missing function parameters, expected {needed}')
