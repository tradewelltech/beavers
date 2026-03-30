from beavers.dag import Dag, Node, TimerManager

try:
    from beavers._version import __version__
except ModuleNotFoundError:
    __version__ = "0.0.0"

__all__ = ["Dag", "Node", "TimerManager"]
