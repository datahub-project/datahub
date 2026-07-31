import typing as t

import sqlglot.expressions

# Add cooperative-timeout support to sqlglot's deep-copy path so long-running SQL
# parsing can be interrupted. This is the only behaviour we override in sqlglot.

_original_deepcopy = sqlglot.expressions.Expression.__deepcopy__


def _deepcopy_wrapper(
    self: sqlglot.expressions.Expression, memo: t.Any
) -> sqlglot.expressions.Expr:
    # copy()/sql() go through __deepcopy__, so this is where we cooperatively check
    # for a timeout. expression_core.py is mypyc-compiled to a .so in sqlglot[c], so
    # we wrap the bound method rather than editing its source.
    import datahub.utilities.cooperative_timeout

    datahub.utilities.cooperative_timeout.cooperate()
    return _original_deepcopy(self, memo)


sqlglot.expressions.Expression.__deepcopy__ = _deepcopy_wrapper  # type: ignore

SQLGLOT_PATCHED = True
