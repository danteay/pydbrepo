"""Mongo query helpers."""

from typing import AnyStr, Optional

from pymongo.collection import Cursor

__all__ = [
    'add_limit',
    'add_offset',
    'add_order_by',
]


def add_limit(query: Cursor, limit: Optional[int] = None) -> Cursor:
    """Add limit filter to a mongo query.

    :param query: Current mongo query cursor
    :param limit: Number of records that will be returned by the query
    :return Cursor: Updated cursor query
    """

    if limit is not None:
        return query.limit(int(limit))

    return query


def add_order_by(query: Cursor, order_by: Optional[AnyStr] = None, order: Optional[int] = None) -> Cursor:
    """Add order_by filter to a mongo cursor query

    :param query: Current mongo query cursor
    :param order_by: Filed that will be ordered
    :param order: Type od ordering Ascending or descending
    :return Cursor: Updated cursor query
    """

    if order_by is not None:
        if order is not None:
            return query.sort(order_by, order)

        return query.sort(order_by)

    return query


def add_offset(query: Cursor, offset: Optional[int] = None) -> Cursor:
    """Add offset filter to a mongo cursor query

    :param query: Current mongo query cursor
    :param offset: Number of omitted records before return query result
    :return Cursor: Updated cursor query
    """

    if offset is not None:
        return query.skip(offset)

    return query
