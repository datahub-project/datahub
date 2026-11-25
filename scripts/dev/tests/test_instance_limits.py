"""Docker vs Docker-free instance-limit keys: new keys, deprecated fallback, independence."""

import json

import pytest

import datahub_dev as dev


def _write_config(payload: dict) -> None:
    dev.CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    dev.CONFIG_FILE.write_text(json.dumps(payload))


class TestHelperResolution:
    def test_docker_helper_reads_new_key(self):
        assert dev._max_docker_instances({"max_docker_instances": 7}) == 7

    def test_docker_free_helper_reads_new_key(self):
        assert dev._max_docker_free_instances({"max_docker_free_instances": 3}) == 3

    def test_docker_helper_falls_back_to_max_local_instances(self):
        assert dev._max_docker_instances({"max_local_instances": 5}) == 5

    def test_docker_free_helper_falls_back_to_max_local_instances(self):
        assert dev._max_docker_free_instances({"max_local_instances": 5}) == 5

    def test_new_key_wins_over_deprecated_fallback(self):
        config = {"max_docker_instances": 2, "max_local_instances": 9}
        assert dev._max_docker_instances(config) == 2
        config = {"max_docker_free_instances": 1, "max_local_instances": 9}
        assert dev._max_docker_free_instances(config) == 1

    def test_defaults_when_no_keys_set(self):
        assert dev._max_docker_instances({}) == dev.DEFAULT_MAX_DOCKER_INSTANCES
        assert (
            dev._max_docker_free_instances({}) == dev.DEFAULT_MAX_DOCKER_FREE_INSTANCES
        )


class TestLimitsAreIndependent:
    def test_lowering_docker_free_does_not_shrink_docker_range(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        # Docker uses max_docker_instances only; max_docker_free_instances has no effect.
        _write_config({"max_docker_instances": 4, "max_docker_free_instances": 1})
        monkeypatch.setattr(dev, "_is_port_free", lambda port: True)
        registry = {"instances": {"a": {"slot": 0}, "b": {"slot": 1}, "c": {"slot": 2}}}
        assert dev._pick_slot(registry, remote=False) == 3

    def test_lowering_docker_does_not_shrink_docker_free_search(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        # Docker-free allocation searches the full browser-compatible range regardless
        # of the Docker cap.
        _write_config({"max_docker_instances": 1, "max_docker_free_instances": 4})
        monkeypatch.setattr(dev, "_is_port_free", lambda port: True)
        assert dev._pick_slot({"instances": {}}, local_stack=True) == 0


class TestDockerAllocationHonoursNewKey:
    def test_docker_full_raises_at_new_key_limit(self, monkeypatch: pytest.MonkeyPatch):
        _write_config({"max_docker_instances": 2})
        # All registered GMS ports answer: the limit counts running Docker stacks.
        monkeypatch.setattr(dev, "_is_port_free", lambda port: False)
        registry = {"instances": {"a": {"slot": 0}, "b": {"slot": 1}}}
        with pytest.raises(RuntimeError, match="Docker"):
            dev._pick_slot(registry, remote=False)

    def test_docker_error_message_names_new_key(self, monkeypatch: pytest.MonkeyPatch):
        # Exhaust the slot search range without hitting running-stack short-circuit so
        # the exhaustion branch (the one that names the config key) fires.
        _write_config({"max_docker_instances": 1})
        monkeypatch.setattr(dev, "_is_port_free", lambda port: True)
        registry = {
            "instances": {
                f"id{i:06x}": {"slot": i, "ports": dev._compute_ports(i)}
                for i in range(6)
            }
        }
        with pytest.raises(RuntimeError, match="max_docker_instances"):
            dev._pick_slot(registry, remote=False)


class TestDockerFreeRunningLimitHonoursNewKey:
    def test_error_wording_uses_new_key(self):
        with pytest.raises(RuntimeError, match="max_docker_free_instances"):
            dev._check_local_running_limit({}, 0)
