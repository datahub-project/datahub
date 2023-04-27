# On SQLAlchemy 1.4.x, the mypy plugin is built-in.
# However, with SQLAlchemy 1.3.x, it requires the sqlalchemy-stubs package and hence has a separate import.
# This file serves as a thin shim layer that directs mypy to the appropriate plugin implementation.
try:
    from mypy.semanal import SemanticAnalyzer
    from sqlalchemy.ext.mypy.plugin import plugin

    # On SQLAlchemy >=1.4, <=1.4.29, the mypy plugin is incompatible with newer versions of mypy.
    # See https://github.com/sqlalchemy/sqlalchemy/commit/aded8b11d9eccbd1f2b645a94338e34a3d234bc9
    # and https://github.com/sqlalchemy/sqlalchemy/issues/7496.
    # To fix this, we need to patch the mypy plugin interface.
    #
    # We cannot set a min version of SQLAlchemy because of the bigquery SQLAlchemy package.
    # See https://github.com/googleapis/python-bigquery-sqlalchemy/issues/385.
    _named_type_original = SemanticAnalyzer.named_type
    _named_type_translations = {
        "__builtins__.object": "builtins.object",
        "__builtins__.str": "builtins.str",
        "__builtins__.list": "builtins.list",
        "__sa_Mapped": "sqlalchemy.orm.attributes.Mapped",
    }

    def _named_type_shim(self, fullname, *args, **kwargs):
        if fullname in _named_type_translations:
            fullname = _named_type_translations[fullname]

        return _named_type_original(self, fullname, *args, **kwargs)

    SemanticAnalyzer.named_type = _named_type_shim  # type: ignore
except ModuleNotFoundError:
    from sqlmypy import plugin  # type: ignore[no-redef]

__all__ = ["plugin"]
