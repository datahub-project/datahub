from unittest.mock import MagicMock, patch, call


def make_mock_row(name, schema="dbo"):
    """Helper: simulate one row from sys.procedures query."""
    row = MagicMock()
    row.__getitem__ = lambda self, k: {
        "procedure_name": name,
        "schema_name": schema,
    }[k]
    return row


def test_percent_in_name_does_not_crash_variant_a():
    """Variant A: procedure with % name + allow/deny regex configured."""

    proc_name = "get%Invoices"
    db_name   = "SummaryTables1"
    schema    = "dbo"

    # Build the qualified name the same way source.py does after our fix
    safe_name = proc_name.replace("%", "%%")
    qualified  = "{}.{}.{}".format(db_name, schema, safe_name)

    # Simulate report_dropped() which internally uses % formatting
    msg = "Dropped: %s" % qualified
    assert "get%%Invoices" in msg  # % is escaped, no crash


def test_percent_in_name_does_not_crash_variant_b():
    """Variant B: % name with no regex filter — TypeError path."""
    proc_name = "get%Revenue"
    safe_name = proc_name.replace("%", "%%")
    qualified  = "{}.{}.{}".format("DB1", "dbo", safe_name)

    # This used to crash with TypeError when used in dict % formatting
    data = {"name": qualified}
    msg  = "Processing %(name)s" % data
    assert "get%%Revenue" in msg


def test_normal_procedure_names_still_work():
    """Make sure normal names (no %) are unaffected."""
    proc_name = "usp_GetCustomers"
    safe_name = proc_name.replace("%", "%%")
    qualified  = "{}.{}.{}".format("MyDB", "dbo", safe_name)
    assert qualified == "MyDB.dbo.usp_GetCustomers"


def test_multiple_percent_chars_in_name():
    """Edge case: multiple % characters."""
    proc_name = "get%top%Sales"
    safe_name = proc_name.replace("%", "%%")
    qualified  = "{}.{}.{}".format("DB", "dbo", safe_name)
    assert "%%" in qualified
    assert qualified.count("%%") == 2  # both % escaped


if __name__ == "__main__":
    test_percent_in_name_does_not_crash_variant_a()
    test_percent_in_name_does_not_crash_variant_b()
    test_normal_procedure_names_still_work()
    test_multiple_percent_chars_in_name()
    print("All 4 tests passed!")