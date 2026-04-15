"""Unit tests for CacheManager."""

from datahub.ingestion.source.snowplow.utils.cache_manager import CacheManager


class TestCacheManagerBasicOperations:
    """Test basic cache operations."""

    def test_get_returns_none_for_missing_key(self):
        """Test that get returns None for keys that don't exist."""
        cache = CacheManager()
        result = cache.get("nonexistent_key")
        assert result is None

    def test_set_and_get_value(self):
        """Test setting and retrieving a value."""
        cache = CacheManager()
        cache.set("test_key", "test_value")
        result = cache.get("test_key")
        assert result == "test_value"

    def test_set_and_get_complex_value(self):
        """Test setting and retrieving complex objects."""
        cache = CacheManager()
        test_data = {"items": [1, 2, 3], "nested": {"key": "value"}}
        cache.set("complex_key", test_data)
        result = cache.get("complex_key")
        assert result == test_data

    def test_has_returns_false_for_missing_key(self):
        """Test has() returns False for keys that don't exist."""
        cache = CacheManager()
        assert cache.has("nonexistent_key") is False

    def test_has_returns_true_for_existing_key(self):
        """Test has() returns True for keys that exist."""
        cache = CacheManager()
        cache.set("test_key", "test_value")
        assert cache.has("test_key") is True

    def test_has_returns_false_for_none_value(self):
        """Test has() returns False when value is None."""
        cache = CacheManager()
        cache.set("none_key", None)
        # None values are treated as "not cached"
        assert cache.has("none_key") is False


class TestCacheManagerTypedAccess:
    """Test type-safe cache access methods."""

    def test_get_typed_returns_value_with_correct_type(self):
        """Test get_typed returns value when type matches."""
        cache = CacheManager()
        cache.set("string_key", "test_string")
        result = cache.get_typed("string_key", str)
        assert result == "test_string"

    def test_get_typed_returns_none_for_missing_key(self):
        """Test get_typed returns None for missing keys."""
        cache = CacheManager()
        result = cache.get_typed("missing", str)
        assert result is None

    def test_get_typed_returns_none_for_type_mismatch(self):
        """Test get_typed returns None when type doesn't match."""
        cache = CacheManager()
        cache.set("string_key", "test_string")
        result = cache.get_typed("string_key", int)
        assert result is None

    def test_get_typed_works_with_list(self):
        """Test get_typed works with list type."""
        cache = CacheManager()
        cache.set("list_key", [1, 2, 3])
        result = cache.get_typed("list_key", list)
        assert result == [1, 2, 3]

    def test_get_typed_works_with_dict(self):
        """Test get_typed works with dict type."""
        cache = CacheManager()
        cache.set("dict_key", {"a": 1})
        result = cache.get_typed("dict_key", dict)
        assert result == {"a": 1}


class TestCacheManagerClearOperations:
    """Test cache clearing operations."""

    def test_clear_single_cache(self):
        """Test clearing a specific cache entry."""
        cache = CacheManager()
        cache.set("key1", "value1")
        cache.set("key2", "value2")

        cache.clear("key1")

        assert cache.get("key1") is None
        assert cache.get("key2") == "value2"

    def test_clear_all_caches(self):
        """Test clearing all cache entries."""
        cache = CacheManager()
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.set("key3", "value3")

        cache.clear()

        assert cache.get("key1") is None
        assert cache.get("key2") is None
        assert cache.get("key3") is None

    def test_clear_nonexistent_key_does_not_raise(self):
        """Test clearing a nonexistent key doesn't raise an error."""
        cache = CacheManager()
        # Should not raise
        cache.clear("nonexistent_key")


class TestCacheManagerGetOrCompute:
    """Test get_or_compute functionality."""

    def test_get_or_compute_returns_cached_value(self):
        """Test get_or_compute returns cached value without calling compute_fn."""
        cache = CacheManager()
        cache.set("cached_key", "cached_value")

        call_count = 0

        def compute_fn() -> str:
            nonlocal call_count
            call_count += 1
            return "computed_value"

        result = cache.get_or_compute("cached_key", compute_fn, str)

        assert result == "cached_value"
        assert call_count == 0  # compute_fn should not be called

    def test_get_or_compute_computes_and_caches_on_miss(self):
        """Test get_or_compute calls compute_fn on cache miss and caches result."""
        cache = CacheManager()

        call_count = 0

        def compute_fn() -> str:
            nonlocal call_count
            call_count += 1
            return "computed_value"

        result = cache.get_or_compute("new_key", compute_fn, str)

        assert result == "computed_value"
        assert call_count == 1
        # Verify it was cached
        assert cache.get("new_key") == "computed_value"

    def test_get_or_compute_does_not_cache_none(self):
        """Test get_or_compute doesn't cache None values."""
        cache = CacheManager()

        def compute_fn() -> None:
            return None

        result = cache.get_or_compute("none_key", compute_fn, type(None))

        assert result is None
        assert cache.has("none_key") is False

    def test_get_or_compute_caches_empty_list(self):
        """Test get_or_compute caches empty lists (not None)."""
        cache = CacheManager()

        def compute_fn() -> list:
            return []

        result = cache.get_or_compute("empty_list_key", compute_fn, list)

        assert result == []
        # Empty list should be cached (it's not None)
        assert cache.get("empty_list_key") == []

    def test_get_or_compute_recomputes_on_type_mismatch(self):
        """Test get_or_compute recomputes if cached value has wrong type."""
        cache = CacheManager()
        # Set a string value
        cache.set("key", "string_value")

        call_count = 0

        def compute_fn() -> int:
            nonlocal call_count
            call_count += 1
            return 42

        # Request as int - should recompute since cached value is str
        result = cache.get_or_compute("key", compute_fn, int)

        assert result == 42
        assert call_count == 1  # compute_fn was called due to type mismatch


class TestCacheManagerLRUEviction:
    """Test LRU eviction behavior."""

    def test_lru_eviction_on_maxsize(self):
        """Test that oldest entries are evicted when maxsize is reached."""
        cache = CacheManager(maxsize=3)

        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.set("key3", "value3")

        # All 3 should be present
        assert cache.get("key1") == "value1"
        assert cache.get("key2") == "value2"
        assert cache.get("key3") == "value3"

        # Add 4th entry, should evict oldest (key1, but we accessed it so it's now
        # "recent" - key2 was accessed second, then key3, then key1)
        # After accessing all 3, order is key2, key3, key1 (oldest to newest)
        # Actually, we need to be more careful about the access order
        cache2 = CacheManager(maxsize=3)
        cache2.set("key1", "value1")
        cache2.set("key2", "value2")
        cache2.set("key3", "value3")
        # Now add key4, key1 should be evicted (LRU)
        cache2.set("key4", "value4")

        # key1 should be evicted (oldest)
        assert cache2.get("key1") is None
        assert cache2.get("key2") == "value2"
        assert cache2.get("key3") == "value3"
        assert cache2.get("key4") == "value4"


class TestCacheManagerRepr:
    """Test string representation."""

    def test_repr_shows_cache_keys(self):
        """Test __repr__ shows cache keys."""
        cache = CacheManager()
        cache.set("key1", "value1")
        cache.set("key2", "value2")

        repr_str = repr(cache)

        assert "CacheManager" in repr_str
        assert "key1" in repr_str
        assert "key2" in repr_str
