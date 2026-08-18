from datahub.utilities.urn_alias.index import (
    CatalogSlice,
    UrnAliasIndex,
    lowercased_urn,
)

_LOWER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.events,PROD)"
_UPPER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,MY_DB.MY_SCHEMA.EVENTS,PROD)"
_MIXED = "urn:li:dataset:(urn:li:dataPlatform:snowflake,My_Db.My_Schema.Events,PROD)"
_OTHER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.orders,PROD)"
_DEV = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.events,DEV)"
_REDSHIFT = "urn:li:dataset:(urn:li:dataPlatform:redshift,my_db.my_schema.events,PROD)"
# A dataset in platform instance `prod_wh`: the instance is fused into the name as a
# leading prefix, with nothing marking where it ends.
_INSTANCED = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod_wh.my_db.my_schema.events,PROD)"
)

_SNOWFLAKE_PROD = CatalogSlice(platform="snowflake", platform_instance=None, env="PROD")


def _store() -> UrnAliasIndex:
    return UrnAliasIndex()


# --- the key space ------------------------------------------------------------------


def test_every_casing_of_a_name_collapses_onto_one_key() -> None:
    assert lowercased_urn(_UPPER) == _LOWER
    assert lowercased_urn(_MIXED) == _LOWER


def test_a_non_dataset_urn_has_no_key() -> None:
    # Nothing to reconcile, so it is never looked up or asked about as a dataset.
    assert lowercased_urn("urn:li:corpuser:jdoe") is None


# --- storing ------------------------------------------------------------------------


def test_a_key_keeps_every_urn_added_under_it() -> None:
    # Two datasets differing only by case can both exist, and a caller has to see the
    # ambiguity rather than be handed an arbitrary winner.
    store = _store()

    store.add("key", _LOWER)
    store.add("key", _UPPER)

    assert store.get("key") == [_LOWER, _UPPER]


def test_the_same_urn_is_stored_once() -> None:
    store = _store()

    store.add("key", _LOWER)
    store.add("key", _LOWER)

    assert store.get("key") == [_LOWER]


def test_an_unwritten_key_is_unknown_rather_than_absent() -> None:
    # The distinction the whole design rests on: unknown has to be asked about, and a
    # recorded absence must not be.
    store = _store()

    store.replace("absent", [])

    assert store.get("unwritten") is None
    assert store.get("absent") == []


def test_replace_makes_the_given_urns_the_whole_answer() -> None:
    store = _store()
    store.add("key", _LOWER)

    store.replace("key", [_UPPER])

    assert store.get("key") == [_UPPER]


def test_the_stored_list_is_not_handed_out() -> None:
    store = _store()
    store.add("key", _LOWER)

    entry = store.get("key")
    assert entry is not None
    entry.append(_OTHER)

    assert store.get("key") == [_LOWER]


# --- coverage ------------------------------------------------------------------------


def test_coverage_is_scoped_by_platform_and_env() -> None:
    key = lowercased_urn(_LOWER) or ""

    assert _SNOWFLAKE_PROD.covers(key)
    assert not _SNOWFLAKE_PROD.covers(lowercased_urn(_REDSHIFT) or "")
    assert not _SNOWFLAKE_PROD.covers(lowercased_urn(_DEV) or "")


def test_no_configured_instance_means_every_instance_was_loaded() -> None:
    # platform_instance=None puts no instance filter on the scroll, so it fetched
    # instanced and instance-less datasets alike.
    assert _SNOWFLAKE_PROD.covers(lowercased_urn(_INSTANCED) or "")


def test_an_empty_instance_covers_the_same_urns_as_no_instance() -> None:
    # `""` puts no instance filter on the scroll either — the filter it builds tests
    # truthiness — so that load fetched the whole platform, instanced datasets included.
    slice_ = CatalogSlice(platform="snowflake", platform_instance="", env="PROD")

    assert slice_.covers(lowercased_urn(_LOWER) or "")
    assert slice_.covers(lowercased_urn(_INSTANCED) or "")


def test_a_configured_instance_covers_only_its_own_name_prefix() -> None:
    slice_ = CatalogSlice(platform="snowflake", platform_instance="PROD_WH", env="PROD")

    # Matched on the lowercased form, so it agrees with the key space lookups use.
    assert slice_.covers(lowercased_urn(_INSTANCED) or "")
    assert not slice_.covers(lowercased_urn(_LOWER) or "")
