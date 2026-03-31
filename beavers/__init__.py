"""Beavers: Python stream processing, optimized for analytics."""

from beavers.dag import Dag, Node, TimerManager

from beavers._version import __version__

__all__ = [
    "Dag",
    "Node",
    "TimerManager",
    "__version__",
]
