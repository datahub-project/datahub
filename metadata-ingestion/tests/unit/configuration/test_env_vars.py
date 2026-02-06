import os
from unittest.mock import patch

from datahub.configuration.env_vars import is_ci


def test_is_ci_with_ci_true():
    """Test that is_ci() returns True when CI=true"""
    with patch.dict(os.environ, {"CI": "true"}, clear=False):
        assert is_ci() is True


def test_is_ci_with_ci_1():
    """Test that is_ci() returns True when CI=1"""
    with patch.dict(os.environ, {"CI": "1"}, clear=False):
        assert is_ci() is True


def test_is_ci_with_ci_yes():
    """Test that is_ci() returns True when CI=yes"""
    with patch.dict(os.environ, {"CI": "yes"}, clear=False):
        assert is_ci() is True


def test_is_ci_with_ci_mixed_case():
    """Test that is_ci() handles mixed case (CI=True, CI=YES)"""
    with patch.dict(os.environ, {"CI": "True"}, clear=False):
        assert is_ci() is True

    with patch.dict(os.environ, {"CI": "YES"}, clear=False):
        assert is_ci() is True


def test_is_ci_with_github_actions():
    """Test that is_ci() returns True when GITHUB_ACTIONS=true"""
    with patch.dict(os.environ, {"GITHUB_ACTIONS": "true"}, clear=True):
        assert is_ci() is True


def test_is_ci_with_github_actions_fallback():
    """Test that is_ci() falls back to GITHUB_ACTIONS when CI is not truthy"""
    with patch.dict(os.environ, {"CI": "false", "GITHUB_ACTIONS": "true"}, clear=True):
        assert is_ci() is True


def test_is_ci_false_when_no_indicators():
    """Test that is_ci() returns False when no CI indicators are present"""
    with patch.dict(os.environ, {}, clear=True):
        assert is_ci() is False


def test_is_ci_false_with_invalid_ci_value():
    """Test that is_ci() returns False when CI has non-truthy value"""
    with patch.dict(os.environ, {"CI": "false"}, clear=True):
        assert is_ci() is False

    with patch.dict(os.environ, {"CI": "0"}, clear=True):
        assert is_ci() is False

    with patch.dict(os.environ, {"CI": "no"}, clear=True):
        assert is_ci() is False


def test_is_ci_false_with_github_actions_not_true():
    """Test that is_ci() returns False when GITHUB_ACTIONS is set but not 'true'"""
    with patch.dict(os.environ, {"GITHUB_ACTIONS": "false"}, clear=True):
        assert is_ci() is False

    with patch.dict(os.environ, {"GITHUB_ACTIONS": "1"}, clear=True):
        assert is_ci() is False


def test_is_ci_with_empty_ci_value():
    """Test that is_ci() returns False when CI is empty string"""
    with patch.dict(os.environ, {"CI": ""}, clear=True):
        assert is_ci() is False


def test_is_ci_with_ci_and_github_actions_both_true():
    """Test that is_ci() returns True when both CI and GITHUB_ACTIONS are true"""
    with patch.dict(os.environ, {"CI": "true", "GITHUB_ACTIONS": "true"}, clear=True):
        assert is_ci() is True
