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


# --- storing ----------------------------------------------------------------------


def test_every_casing_of_a_name_collapses_onto_one_key() -> None:
    index = UrnAliasIndex()

    index.add(_LOWER)

    assert index.lookup(_UPPER) == [_LOWER]
    assert index.lookup(_MIXED) == [_LOWER]


def test_a_case_collision_keeps_both_urns() -> None:
    # Two real entities differing only by case: both come back, so the caller can see
    # the ambiguity rather than being handed an arbitrary winner.
    index = UrnAliasIndex()

    index.add(_LOWER)
    index.add(_UPPER)

    assert index.lookup(_MIXED) == [_LOWER, _UPPER]


def test_the_same_urn_is_recorded_once() -> None:
    index = UrnAliasIndex()

    index.add(_LOWER)
    index.add(_LOWER)

    assert index.lookup(_LOWER) == [_LOWER]


def test_the_stored_list_is_not_handed_out() -> None:
    index = UrnAliasIndex()
    index.add(_LOWER)

    entry = index.lookup(_LOWER)
    assert entry is not None
    entry.append(_OTHER)

    assert index.lookup(_LOWER) == [_LOWER]


def test_a_non_dataset_urn_is_answered_not_deferred() -> None:
    # There is no casing to reconcile, so this is a definite answer: querying could not
    # help, and returning "unknown" would send it to the server for nothing.
    assert UrnAliasIndex().lookup("urn:li:corpuser:jdoe") == []


# --- the four ways a lookup is satisfied --------------------------------------------


def test_a_stored_match_answers() -> None:
    index = UrnAliasIndex()
    index.add(_LOWER)

    assert index.lookup(_UPPER) == [_LOWER]


def test_a_recorded_absence_answers() -> None:
    index = UrnAliasIndex()

    index.record_matches(lowercased_urn(_OTHER) or "", [])

    assert index.lookup(_OTHER) == []


def test_a_miss_inside_a_loaded_slice_is_an_answer_not_a_gap() -> None:
    # The property the whole design rests on: we scrolled this slice to completion, so
    # nothing in it went unseen, so "not stored" means DataHub does not hold it.
    index = UrnAliasIndex()
    index.add(_LOWER)
    index.record_slice_loaded(_SNOWFLAKE_PROD)

    assert index.lookup(_OTHER) == []


def test_a_miss_outside_every_loaded_slice_stays_unknown() -> None:
    index = UrnAliasIndex()
    index.add(_LOWER)
    index.record_slice_loaded(_SNOWFLAKE_PROD)

    # None, not []: nothing here has ever looked at redshift or at DEV, so the only
    # honest answer is that we do not know.
    assert index.lookup(_REDSHIFT) is None
    assert index.lookup(_DEV) is None


def test_an_unloaded_index_knows_nothing() -> None:
    assert UrnAliasIndex().lookup(_LOWER) is None


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


def test_a_configured_instance_covers_only_its_own_name_prefix() -> None:
    slice_ = CatalogSlice(platform="snowflake", platform_instance="PROD_WH", env="PROD")

    # Matched on the lowercased form, so it agrees with the key space lookups use.
    assert slice_.covers(lowercased_urn(_INSTANCED) or "")
    assert not slice_.covers(lowercased_urn(_LOWER) or "")


def test_coverage_is_recorded_once_per_slice() -> None:
    index = UrnAliasIndex()

    index.record_slice_loaded(_SNOWFLAKE_PROD)
    index.record_slice_loaded(_SNOWFLAKE_PROD)

    assert index._loaded_slices == [_SNOWFLAKE_PROD]
