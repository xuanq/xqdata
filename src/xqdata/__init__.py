import importlib.metadata

from xqdata.dataapi import get_dataapi

__version__ = importlib.metadata.version(__name__)

__all__ = ["get_dataapi", "__version__"]
