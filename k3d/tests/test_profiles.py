"""Tests for dh.profiles — profile resolution and set args."""

from __future__ import annotations

from pathlib import Path

import pytest

from dh.profiles import PROFILES, _profile_set_args, resolve_profiles


class TestProfilesConstant:
    def test_contents(self):
        assert PROFILES == ("minimal", "consumers", "backend")


class TestResolveProfiles:
    def test_single_profile(self, profiles_dir):
        values, set_args = resolve_profiles(("minimal",), profiles_dir)
        assert len(values) == 1
        assert values[0] == profiles_dir / "minimal.yaml"
        assert "--set" in set_args

    def test_multiple_profiles(self, profiles_dir):
        values, set_args = resolve_profiles(
            ("minimal", "consumers"), profiles_dir
        )
        assert len(values) == 2
        assert values[0] == profiles_dir / "minimal.yaml"
        assert values[1] == profiles_dir / "consumers.yaml"

    def test_backend_profile(self, profiles_dir):
        values, set_args = resolve_profiles(("backend",), profiles_dir)
        assert len(values) == 1
        assert values[0] == profiles_dir / "backend.yaml"
        # backend has no set args
        assert set_args == []

    def test_missing_yaml_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="Profile values file not found"):
            resolve_profiles(("minimal",), tmp_path)

    def test_order_preserved(self, profiles_dir):
        values, _ = resolve_profiles(
            ("consumers", "minimal", "backend"), profiles_dir
        )
        assert [v.stem for v in values] == ["consumers", "minimal", "backend"]


class TestProfileSetArgs:
    def test_minimal_sets_index_3(self):
        result = _profile_set_args("minimal")
        assert result == ["--set", "datahub-gms.extraEnvs[3].value=false"]

    def test_consumers_sets_indices_0_1_2(self):
        result = _profile_set_args("consumers")
        assert len(result) == 6  # 3 pairs of --set + value
        values = [result[i] for i in range(1, 6, 2)]
        assert "datahub-gms.extraEnvs[0].value=false" in values
        assert "datahub-gms.extraEnvs[1].value=false" in values
        assert "datahub-gms.extraEnvs[2].value=false" in values

    def test_backend_has_no_set_args(self):
        assert _profile_set_args("backend") == []

    def test_no_index_overlap(self):
        """Minimal and consumers profiles should not touch the same indices."""
        minimal_args = _profile_set_args("minimal")
        consumers_args = _profile_set_args("consumers")

        def extract_indices(args):
            indices = set()
            for a in args:
                if "extraEnvs[" in a:
                    idx = a.split("[")[1].split("]")[0]
                    indices.add(int(idx))
            return indices

        m_idx = extract_indices(minimal_args)
        c_idx = extract_indices(consumers_args)
        assert m_idx.isdisjoint(c_idx), f"Overlap: {m_idx & c_idx}"
