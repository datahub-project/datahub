from datahub.utilities.urn_alias_resolver import UrnAliasCache, UrnAliasResolver

_LOWER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.events,PROD)"
_UPPER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,MY_DB.MY_SCHEMA.EVENTS,PROD)"
_MIXED = "urn:li:dataset:(urn:li:dataPlatform:snowflake,My_Db.My_Schema.Events,PROD)"
_OTHER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.orders,PROD)"


def _loaded(*urns: str) -> UrnAliasResolver:
    resolver = UrnAliasResolver()
    for urn in urns:
        resolver.add(urn)
    return resolver


# --- cache ------------------------------------------------------------------------


def test_cache_matches_a_different_casing() -> None:
    cache = UrnAliasCache()
    cache.add(_LOWER)

    assert cache.get(_UPPER) == [_LOWER]


def test_cache_distinguishes_unknown_from_known_absent() -> None:
    cache = UrnAliasCache()
    cache.add(_LOWER)

    # None means unknown; an empty list would mean known not to exist.
    assert cache.get(_OTHER) is None


def test_cache_records_the_same_urn_once() -> None:
    cache = UrnAliasCache()
    cache.add(_LOWER)
    cache.add(_LOWER)

    assert cache.get(_LOWER) == [_LOWER]
    assert cache.count() == 1


def test_cache_does_not_hand_out_its_stored_list() -> None:
    cache = UrnAliasCache()
    cache.add(_LOWER)

    entry = cache.get(_UPPER)
    assert entry is not None
    entry.append(_OTHER)

    assert cache.get(_UPPER) == [_LOWER]


# --- resolver ---------------------------------------------------------------------


def test_lookup_matches_a_different_casing() -> None:
    resolver = _loaded(_LOWER)

    assert resolver.lookup(_UPPER) == [_LOWER]
    assert resolver.lookup(_MIXED) == [_LOWER]


def test_lookup_returns_the_stored_urn_for_an_exact_match() -> None:
    resolver = _loaded(_MIXED)

    assert resolver.lookup(_MIXED) == [_MIXED]


def test_lookup_returns_every_urn_of_a_case_collision() -> None:
    # Two real entities differing only by case: both are returned so the caller can
    # see the ambiguity.
    resolver = _loaded(_LOWER, _UPPER)

    assert resolver.lookup(_MIXED) == [_LOWER, _UPPER]


def test_lookup_flattens_unknown_and_absent_to_empty() -> None:
    resolver = _loaded(_LOWER)

    # Callers get one "no match" answer and never have to interpret None.
    assert resolver.lookup(_OTHER) == []


def test_count_is_the_number_of_urns_recorded() -> None:
    resolver = _loaded(_LOWER, _UPPER, _LOWER)

    assert resolver.count() == 2
