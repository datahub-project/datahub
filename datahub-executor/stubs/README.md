# DataHub Mypy Stubs

This directory contains mypy stub files (`.pyi`) that extend DataHub's base modules with acryl-cloud specific classes.

## Why are these stubs needed?

DataHub's `schema_classes.py` and `urns.py` modules use dynamic imports to load custom packages at runtime:

```python
_custom_package_path = get_custom_models_package()

if TYPE_CHECKING or not _custom_package_path:
    from ._internal_schema_classes import *
    # ... only base classes are imported
else:
    _custom_package = importlib.import_module(_custom_package_path)
    globals().update(_custom_package.__dict__)
    # ... custom package classes are dynamically added
```

This dynamic behavior works at runtime but is not effective with mypy's static analysis. Mypy cannot resolve the dynamically imported classes from `acryl_datahub_cloud.metadata.schema_classes` and `acryl_datahub_cloud.metadata._urns.urn_defs`.

## Solution

These stub files force mypy to recognize both the base DataHub classes and the acryl-cloud extensions:

- **`schema_classes.pyi`** - Extends `datahub.metadata.schema_classes` with classes from `acryl_datahub_cloud.metadata.schema_classes`
- **`urns.pyi`** - Extends `datahub.metadata.urns` with URNs from `acryl_datahub_cloud.metadata._urns.urn_defs`

## Configuration

The stubs are configured in `mypy.ini`:

```ini
[mypy]
mypy_path = src:stubs
```

This allows mypy to find and use these stub definitions during type checking, resolving import errors for custom classes like `MonitorInfoClass`, `SubscriptionUrn`, etc.
