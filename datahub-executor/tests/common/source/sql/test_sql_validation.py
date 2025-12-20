import pytest

from datahub_executor.common.exceptions import InvalidParametersException
from datahub_executor.common.source.sql.utils import validate_sql_is_select_only


class TestSQLValidation:
    """Tests for sqlglot-based SQL validation."""

    def test_valid_simple_select(self) -> None:
        """Valid SELECT statement should pass."""
        sql = "SELECT * FROM users"
        validate_sql_is_select_only(sql, "test")  # Should not raise

    def test_valid_select_with_where(self) -> None:
        """SELECT with WHERE clause should pass."""
        sql = "SELECT id, name FROM users WHERE active = true"
        validate_sql_is_select_only(sql, "test")  # Should not raise

    def test_valid_select_with_join(self) -> None:
        """SELECT with JOIN should pass."""
        sql = """
            SELECT u.id, u.name, o.order_id
            FROM users u
            JOIN orders o ON u.id = o.user_id
        """
        validate_sql_is_select_only(sql, "test")  # Should not raise

    def test_valid_select_with_cte(self) -> None:
        """SELECT with CTE (WITH clause) should pass."""
        sql = """
            WITH active_users AS (
                SELECT * FROM users WHERE active = true
            )
            SELECT * FROM active_users
        """
        validate_sql_is_select_only(sql, "test")  # Should not raise

    def test_valid_select_with_trailing_semicolon(self) -> None:
        """SELECT with trailing semicolon should pass."""
        sql = "SELECT * FROM users;"
        validate_sql_is_select_only(sql, "test")  # Should not raise

    def test_valid_select_with_subquery(self) -> None:
        """SELECT with subquery should pass."""
        sql = """
            SELECT * FROM users
            WHERE id IN (SELECT user_id FROM active_sessions)
        """
        validate_sql_is_select_only(sql, "test")  # Should not raise

    def test_reject_drop_table(self) -> None:
        """DROP TABLE should be rejected."""
        sql = "DROP TABLE users"
        with pytest.raises(InvalidParametersException) as exc_info:
            validate_sql_is_select_only(sql, "test")
        assert "must be a SELECT statement" in exc_info.value.message
        assert "Drop" in exc_info.value.message

    def test_reject_delete(self) -> None:
        """DELETE should be rejected."""
        sql = "DELETE FROM users WHERE id = 1"
        with pytest.raises(InvalidParametersException) as exc_info:
            validate_sql_is_select_only(sql, "test")
        assert "must be a SELECT statement" in exc_info.value.message
        assert "Delete" in exc_info.value.message

    def test_reject_insert(self) -> None:
        """INSERT should be rejected."""
        sql = "INSERT INTO users (name) VALUES ('John')"
        with pytest.raises(InvalidParametersException) as exc_info:
            validate_sql_is_select_only(sql, "test")
        assert "must be a SELECT statement" in exc_info.value.message
        assert "Insert" in exc_info.value.message

    def test_reject_update(self) -> None:
        """UPDATE should be rejected."""
        sql = "UPDATE users SET name = 'Jane' WHERE id = 1"
        with pytest.raises(InvalidParametersException) as exc_info:
            validate_sql_is_select_only(sql, "test")
        assert "must be a SELECT statement" in exc_info.value.message
        assert "Update" in exc_info.value.message

    def test_reject_create_table(self) -> None:
        """CREATE TABLE should be rejected."""
        sql = "CREATE TABLE users (id INT, name VARCHAR(100))"
        with pytest.raises(InvalidParametersException) as exc_info:
            validate_sql_is_select_only(sql, "test")
        assert "must be a SELECT statement" in exc_info.value.message
        assert "Create" in exc_info.value.message

    def test_reject_alter_table(self) -> None:
        """ALTER TABLE should be rejected."""
        sql = "ALTER TABLE users ADD COLUMN email VARCHAR(255)"
        with pytest.raises(InvalidParametersException) as exc_info:
            validate_sql_is_select_only(sql, "test")
        assert "must be a SELECT statement" in exc_info.value.message
        assert "Alter" in exc_info.value.message

    def test_reject_truncate(self) -> None:
        """TRUNCATE should be rejected."""
        sql = "TRUNCATE TABLE users"
        with pytest.raises(InvalidParametersException) as exc_info:
            validate_sql_is_select_only(sql, "test")
        assert "must be a SELECT statement" in exc_info.value.message

    def test_reject_empty_sql(self) -> None:
        """Empty SQL should be rejected."""
        with pytest.raises(InvalidParametersException) as exc_info:
            validate_sql_is_select_only("", "test")
        assert "cannot be empty" in exc_info.value.message

    def test_reject_whitespace_only(self) -> None:
        """Whitespace-only SQL should be rejected."""
        with pytest.raises(InvalidParametersException) as exc_info:
            validate_sql_is_select_only("   \n  ", "test")
        assert "cannot be empty" in exc_info.value.message

    def test_context_in_error_message(self) -> None:
        """Context should appear in error messages."""
        with pytest.raises(InvalidParametersException) as exc_info:
            validate_sql_is_select_only("DROP TABLE users", "my custom context")
        assert "my custom context" in exc_info.value.message

    def test_sql_preview_in_error(self) -> None:
        """SQL preview should be included in error parameters."""
        sql = "DELETE FROM users WHERE id = 1"
        with pytest.raises(InvalidParametersException) as exc_info:
            validate_sql_is_select_only(sql, "test")
        # parameters is stored as string in InvalidParametersException
        assert "sql" in exc_info.value.parameters
        assert sql in exc_info.value.parameters

    def test_reject_invalid_sql_syntax(self) -> None:
        """Invalid SQL syntax that doesn't look like SELECT should be rejected."""
        sql = "INVALID SQL STATEMENT"  # Doesn't start with SELECT
        with pytest.raises(InvalidParametersException) as exc_info:
            validate_sql_is_select_only(sql, "test")
        assert "invalid SQL" in exc_info.value.message

    def test_reject_cte_with_insert(self) -> None:
        """CTE containing INSERT should be rejected."""
        sql = """
            WITH inserted AS (
                INSERT INTO users (name) VALUES ('John') RETURNING *
            )
            SELECT * FROM inserted
        """
        with pytest.raises(InvalidParametersException) as exc_info:
            validate_sql_is_select_only(sql, "test")
        # This should catch the non-SELECT in CTE
        assert (
            "CTE" in exc_info.value.message
            or "must be a SELECT" in exc_info.value.message
        )
