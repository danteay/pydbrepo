"""Query errors."""


class QueryError(Exception):
    """for query execution failures or miss configurations."""


class BuilderError(Exception):
    """Exception for any of the build steps of the query inside a repository"""


class DriverExecutionError(Exception):
    """For any issue found on query execution."""
