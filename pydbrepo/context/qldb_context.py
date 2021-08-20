"""QLDB query context that help to store results of the queries that will be executed
by the driver.
"""

from contextlib import ContextDecorator


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

