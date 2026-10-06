from lib.graphql import check_assertions


def test_assertions_pass() -> None:
    data = {"domain": {"privileges": {"canViewEntity": True}}}
    assert not check_assertions(data, {"domain.privileges.canViewEntity": True})


def test_assertions_fail() -> None:
    data = {"domain": {"privileges": {"canViewEntity": False}}}
    failures = check_assertions(data, {"domain.privileges.canViewEntity": True})
    assert len(failures) == 1
