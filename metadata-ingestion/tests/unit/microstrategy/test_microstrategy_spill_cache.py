from datahub.ingestion.source.microstrategy.spill_cache import SpillCache


def test_a_stored_none_is_distinct_from_a_missing_key() -> None:
    cache: SpillCache[object] = SpillCache(memory_entries=4)
    cache.put("failed", None)

    assert "failed" in cache
    assert cache.get("failed") is None
    assert "unknown" not in cache
    assert cache.get("unknown") is None
    cache.close()


def test_compressed_values_round_trip_after_spilling_to_disk() -> None:
    cache: SpillCache[str] = SpillCache(memory_entries=2, compress=True)
    values = {f"key-{index}": f"select {index} " * 50 for index in range(8)}

    for key, value in values.items():
        cache.put(key, value)

    store = cache._store
    assert len(store._active_object_cache) <= 2
    assert store.sql_query(f"SELECT COUNT(*) FROM {store.tablename}")[0][0] >= 6
    assert {key: cache.get(key) for key in values} == values
    cache.close()


def test_reset_drops_every_entry_and_the_cache_stays_usable() -> None:
    cache: SpillCache[str] = SpillCache(memory_entries=2)
    for index in range(6):
        cache.put(f"key-{index}", "value")

    cache.reset()

    assert all(f"key-{index}" not in cache for index in range(6))
    cache.put("again", "value")
    assert cache.get("again") == "value"
    cache.close()
