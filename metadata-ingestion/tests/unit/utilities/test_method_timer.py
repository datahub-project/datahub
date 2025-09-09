import time
from typing import Dict

from datahub.utilities.method_timer import timed_method


class TestMethodTimer:
    def test_basic_timing(self):
        class TestClass:
            def __init__(self):
                self.method_timings_sec: Dict[str, float] = {}

            @timed_method("test_method")
            def test_method(self):
                time.sleep(0.01)
                return "result"

        obj = TestClass()
        result = obj.test_method()

        assert result == "result"
        assert obj.method_timings_sec["test_method"] >= 0.01

    def test_timing_accumulation(self):
        class TestClass:
            def __init__(self):
                self.method_timings_sec: Dict[str, float] = {}

            @timed_method("shared_key")
            def method_a(self):
                time.sleep(0.01)
                return "a"

            @timed_method("shared_key")
            def method_b(self):
                time.sleep(0.01)
                return "b"

        obj = TestClass()
        obj.method_a()
        first_time = obj.method_timings_sec["shared_key"]

        obj.method_b()
        second_time = obj.method_timings_sec["shared_key"]

        assert second_time > first_time
        assert second_time >= first_time + 0.01

    def test_multiple_timers(self):
        class TestClass:
            def __init__(self):
                self.method_timings_sec: Dict[str, float] = {}

            @timed_method("timer_a")
            def method_a(self):
                time.sleep(0.01)

            @timed_method("timer_b")
            def method_b(self):
                time.sleep(0.01)

        obj = TestClass()
        obj.method_a()
        obj.method_b()

        assert obj.method_timings_sec["timer_a"] >= 0.01
        assert obj.method_timings_sec["timer_b"] >= 0.01
