import importlib
import pkgutil
import inspect
import os

from datahub.ingestion.api.registry import Registry
from datahub.ingestion.api.source import Source

source_registry = Registry[Source]()

source_module = os.path.dirname(__file__)
for plugin in pkgutil.iter_modules([source_module]):
    try:
        # Try to import plugin module
        module = importlib.import_module('..' + plugin.name, __loader__.name)

        # Get all non-abstract Source classes from module
        source_classes = [
            class_ for _, class_ in inspect.getmembers(module, inspect.isclass)
            if issubclass(class_, Source) and not inspect.isabstract(class_)
        ]

        # Register plugin with first discovered class
        if source_classes:
            source_registry.register(plugin.name, source_classes[0])

    except ImportError as e:
        source_registry.register_disabled(plugin.name, e)
