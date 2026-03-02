"""Tests for dh.template — template expansion."""

from __future__ import annotations

import pytest

from dh.template import expand_template


class TestExpandTemplate:
    def test_substitutes_variables(self, tmp_path):
        tpl = tmp_path / "test.tpl"
        tpl.write_text("Hello ${NAME}, port=${PORT}")
        result = expand_template(tpl, {"NAME": "world", "PORT": "8080"})
        assert result == "Hello world, port=8080"

    def test_missing_variable_raises(self, tmp_path):
        tpl = tmp_path / "test.tpl"
        tpl.write_text("Hello ${NAME}")
        with pytest.raises(KeyError):
            expand_template(tpl, {})

    def test_extra_variables_ignored(self, tmp_path):
        tpl = tmp_path / "test.tpl"
        tpl.write_text("Hello ${NAME}")
        result = expand_template(tpl, {"NAME": "world", "UNUSED": "ignored"})
        assert result == "Hello world"

    def test_dollar_without_braces_literal(self, tmp_path):
        tpl = tmp_path / "test.tpl"
        tpl.write_text("$$escaped and ${VAR}")
        result = expand_template(tpl, {"VAR": "ok"})
        assert result == "$escaped and ok"
