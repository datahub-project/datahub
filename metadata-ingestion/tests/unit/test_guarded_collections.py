import threading
from concurrent.futures import ThreadPoolExecutor

from datahub.utilities.guarded_collections import GuardedDict, GuardedSet


class TestGuardedSet:
    def test_check_and_add_returns_true_for_new_item(self) -> None:
        gs: GuardedSet[str] = GuardedSet()
        assert gs.check_and_add("a") is True

    def test_check_and_add_returns_false_for_existing_item(self) -> None:
        gs: GuardedSet[str] = GuardedSet()
        gs.check_and_add("a")
        assert gs.check_and_add("a") is False

    def test_add_and_contains(self) -> None:
        gs: GuardedSet[str] = GuardedSet()
        gs.add("x")
        assert "x" in gs
        assert "y" not in gs

    def test_len(self) -> None:
        gs: GuardedSet[int] = GuardedSet()
        gs.add(1)
        gs.add(2)
        gs.add(2)
        assert len(gs) == 2

    def test_iter_returns_snapshot(self) -> None:
        gs: GuardedSet[str] = GuardedSet()
        gs.add("a")
        gs.add("b")
        assert sorted(gs) == ["a", "b"]

    def test_concurrent_check_and_add_no_duplicates(self) -> None:
        gs: GuardedSet[int] = GuardedSet()
        results: list = []
        lock = threading.Lock()

        def worker(value: int) -> None:
            added = gs.check_and_add(value)
            with lock:
                results.append((value, added))

        with ThreadPoolExecutor(max_workers=8) as pool:
            for i in range(100):
                pool.submit(worker, i % 10)

        added_true = [v for v, added in results if added]
        assert sorted(added_true) == list(range(10))
        assert len(gs) == 10


class TestGuardedDict:
    def test_setitem_and_getitem(self) -> None:
        gd: GuardedDict[str, int] = GuardedDict()
        gd["a"] = 1
        assert gd["a"] == 1

    def test_get_with_default(self) -> None:
        gd: GuardedDict[str, int] = GuardedDict()
        assert gd.get("missing") is None
        assert gd.get("missing", 42) == 42

    def test_contains(self) -> None:
        gd: GuardedDict[str, int] = GuardedDict()
        gd["x"] = 1
        assert "x" in gd
        assert "y" not in gd

    def test_len(self) -> None:
        gd: GuardedDict[str, int] = GuardedDict()
        gd["a"] = 1
        gd["b"] = 2
        assert len(gd) == 2

    def test_setdefault(self) -> None:
        gd: GuardedDict[str, int] = GuardedDict()
        assert gd.setdefault("a", 10) == 10
        assert gd.setdefault("a", 99) == 10

    def test_values_returns_snapshot(self) -> None:
        gd: GuardedDict[str, int] = GuardedDict()
        gd["a"] = 1
        gd["b"] = 2
        assert sorted(gd.values()) == [1, 2]

    def test_items_returns_snapshot(self) -> None:
        gd: GuardedDict[str, int] = GuardedDict()
        gd["a"] = 1
        assert gd.items() == [("a", 1)]

    def test_iter_returns_keys(self) -> None:
        gd: GuardedDict[str, int] = GuardedDict()
        gd["x"] = 1
        gd["y"] = 2
        assert sorted(gd) == ["x", "y"]

    def test_compute_if_absent_creates_value(self) -> None:
        gd: GuardedDict[str, list] = GuardedDict()
        result = gd.compute_if_absent("key", lambda k: [k])
        assert result == ["key"]
        assert gd["key"] == ["key"]

    def test_compute_if_absent_returns_existing(self) -> None:
        gd: GuardedDict[str, int] = GuardedDict()
        gd["key"] = 42
        result = gd.compute_if_absent("key", lambda k: 999)
        assert result == 42

    def test_concurrent_compute_if_absent_calls_factory_once(self) -> None:
        gd: GuardedDict[int, int] = GuardedDict()
        call_count = 0
        count_lock = threading.Lock()

        def counting_factory(k: int) -> int:
            nonlocal call_count
            with count_lock:
                call_count += 1
            return k * 10

        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [
                pool.submit(gd.compute_if_absent, 1, counting_factory)
                for _ in range(50)
            ]
            results = [f.result() for f in futures]

        assert all(r == 10 for r in results)
        assert call_count == 1
