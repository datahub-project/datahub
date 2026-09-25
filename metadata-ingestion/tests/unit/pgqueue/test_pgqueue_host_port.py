"""Tests for :mod:`datahub.pgqueue.host_port` (stdlib-only host parsing)."""

from __future__ import annotations

from datahub.pgqueue.host_port import parse_host_port


def test_parse_host_port_explicit_port() -> None:
    assert parse_host_port("localhost:3306") == ("localhost", 3306)


def test_parse_host_port_default_port() -> None:
    assert parse_host_port("localhost", 5432) == ("localhost", 5432)


def test_parse_host_port_invalid_port_segment_uses_default() -> None:
    assert parse_host_port("db.example.com:invalid", 3306) == ("db.example.com", 3306)
