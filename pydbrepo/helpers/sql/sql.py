"""Helper functions for query building."""

from typing import Any, AnyStr, List, Mapping, Optional, Set, Tuple

from pypika import Field, Order, Parameter
from pypika.queries import QueryBuilder

from pydbrepo.drivers.driver import Driver
from pydbrepo.errors import BuilderError
from pydbrepo.helpers import common

__all__ = [
    'add_limit',
    'add_offset',
    'add_group_by',
    'add_order_by',
    'add_returning',
    'add_set_statements',
    'add_where_statements',
]


def add_limit(query: QueryBuilder, params: Mapping) -> QueryBuilder:
    """Add limit filter if there is configured on params.

    :param query: Instance of the pypika query builder
    :param params: Configured params for the query
    :return QueryBuilder: Instance of the query builder
    """

    limit = params.get('limit', None)

    if limit is not None:
        query = query.limit(limit)

    return query


def add_offset(query: QueryBuilder, params: Mapping) -> QueryBuilder:
    """Add offset filter if there is configured on params.

    :param query: Instance of the pypika query builder
    :param params: Configured params for the query
    :return QueryBuilder: Instance of the query builder
    """

    offset = params.get('offset', None)

    if offset is not None:
        query = query.offset(offset)

    return query


def add_order_by(query: QueryBuilder, params: Mapping) -> QueryBuilder:
    """Add order by filters if are configured on params.

    :param query: Instance of the pypika query builder
    :param params: Configured params for the query
    :return QueryBuilder: Instance of the query builder
    """

    order_by = params.get('order_by', None)

    if order_by is not None:
        for order in order_by:
            order_type = order[1] if order[1] else Order.desc
            query = query.orderby(Field(order[0]), order=order_type)

    return query


def add_group_by(query: QueryBuilder, params: Mapping) -> QueryBuilder:
    """Add group by filters if are configured on params.

    :param query: Instance of the pypika query builder
    :param params: Configured params for the query
    :return QueryBuilder: Instance of the query builder
    """

    group_by = params.get('group_by', None)

    if group_by is not None:
        for group in group_by:
            query = query.groupby(Field(group))

    return query


def add_returning(query: QueryBuilder, fields: List[AnyStr]) -> AnyStr:
    """Build the string query to add final RETURNING statement.

    :param query: Current query builder instance
    :param fields: Fields to be returned
    """

    if fields is None:
        return str(query)

    return f"{str(query)} RETURNING {', '.join(fields)}"


def add_set_statements(
    query: QueryBuilder,
    data: Mapping,
    entity_properties: Set[AnyStr],
    driver: Driver,
    **kwargs,
) -> Tuple[QueryBuilder, List[Any]]:
    """Add SET statements of an UPDATE query.

    :param query: Current Query builder
    :param data: Data to be added to update query
    :param entity_properties: Allowed entity properties to be used
    :param driver: Current driver to get placeholder
    :param kwargs: Any other diver placeholder related arguments
    :return QueryBuilder: Updated QueryBuilder
    :raise RepositoryBuilderError: If some data key is not present on the entity properties
    """

    values = []

    for key, value in data.items():
        if key not in entity_properties:
            raise BuilderError(f"Field {key} isn't present on handled entity.")

        values.append(common.handle_extra_types(value))
        query = query.set(Field(key), Parameter(driver.placeholder(**kwargs)))

    return query, values


def add_where_statements(
    query: QueryBuilder,
    data: Mapping,
    entity_properties: Set[AnyStr],
    driver: Driver,
    skip: Optional[Set[AnyStr]] = None,
    **kwargs,
) -> Tuple[QueryBuilder, List[Any]]:
    """Add SET statements of an UPDATE query.

    :param query: Current Query builder
    :param data: Data to be added to update query
    :param entity_properties: Allowed entity properties to be used
    :param skip: Set of field names that will be skipped if they are present on data mapping
    :param driver: Current driver to get placeholder
    :param kwargs: Any other diver placeholder related arguments
    :return QueryBuilder: Updated QueryBuilder
    :raise RepositoryBuilderError: If some data key is not present on the entity properties
    """

    if skip is None:
        skip = set()

    values = []

    for key, value in data.items():
        if key in skip:
            continue

        if key not in entity_properties:
            raise BuilderError(f"Field {key} isn't present on handled entity.")

        values.append(common.handle_extra_types(value))
        query = query.where(Field(key) == Parameter(driver.placeholder(**kwargs)))

    return query, values
