"""Specific Mysql helper functions."""

from typing import AnyStr


def last_inserted_id_query() -> AnyStr:
    """Return Query to retrieve last inserted ID.

    :return AnyStr: SQL query
    """

    return 'SELECT LAST_INSERTED_ID()'
