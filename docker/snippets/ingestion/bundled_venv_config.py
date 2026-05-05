# Copyright 2025 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Shared helpers for bundled venv group resolution (used by build script and tests).

Optional module ``bundled_venv_plugin_extras_local`` may define ``plugin_extras_for_plugin``
for site-specific or downstream overrides; otherwise the OSS default implementation is used.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

# Env keys: BUNDLED_VENV_PLUGINS_<GROUP_ID> where GROUP_ID is any case; group label is
# GROUP_ID lowercased (e.g. BUNDLED_VENV_PLUGINS_COMMON -> group "common").
GROUP_PLUGINS_ENV_PREFIX = "BUNDLED_VENV_PLUGINS_"

# Optional: which group member selects variant-specific auxiliary pip installs (suffix
# aligned with BUNDLED_VENV_PLUGINS_<same>).
GROUP_AUX_PRIMARY_ENV_PREFIX = "BUNDLED_VENV_AUX_PRIMARY_"

# Plugins with PySpark-heavy extras — use -slim in slim mode (matches executor image builds).
PLUGINS_WITH_SLIM_VARIANT = ["s3", "gcs", "abs"]

# Additional extras to install for specific plugins beyond the standard set.
PLUGIN_ADDITIONAL_EXTRAS: Dict[str, List[str]] = {
    "file": ["s3"],
}


def _default_plugin_extras_for_plugin(plugin: str, slim_mode: bool) -> List[str]:
    """OSS default: standard ingestion extras per plugin (no vendor-specific branches)."""
    plugin_extra = plugin
    if slim_mode and plugin in PLUGINS_WITH_SLIM_VARIANT:
        plugin_extra = f"{plugin}-slim"

    extras: List[str] = ["datahub-rest", "datahub-kafka", "file", plugin_extra]
    seen: Set[str] = set()
    out: List[str] = []
    for x in extras:
        if x not in seen:
            seen.add(x)
            out.append(x)

    for additional in PLUGIN_ADDITIONAL_EXTRAS.get(plugin, []):
        add = additional
        if slim_mode and additional in PLUGINS_WITH_SLIM_VARIANT:
            add = f"{additional}-slim"
        if add not in seen:
            seen.add(add)
            out.append(add)
    return out


try:
    import bundled_venv_plugin_extras_local as _plugin_extras_local

    plugin_extras_for_plugin = _plugin_extras_local.plugin_extras_for_plugin
except ImportError:
    plugin_extras_for_plugin = _default_plugin_extras_for_plugin


def sorted_union_extras(plugins: Sequence[str], slim_mode: bool) -> List[str]:
    """Sorted union of extras for all plugins in the group."""
    u: Set[str] = set()
    for p in plugins:
        u.update(plugin_extras_for_plugin(p, slim_mode))
    return sorted(u)


def extras_to_install_string(extras: Sequence[str]) -> str:
    return ",".join(extras)


def groups_config_from_plugin_group_env(
    environ: Mapping[str, str],
) -> Optional[Dict[str, Any]]:
    """
    Build a groups config from BUNDLED_VENV_PLUGINS_<GroupId> variables.

    The suffix after BUNDLED_VENV_PLUGINS_ is normalized to lowercase for the group label
    (canonical dir ``{label}-venv``). Values are comma-separated plugin names.

    Returns None if no such variables are set (every plugin becomes its own singleton venv).
    """
    groups: Dict[str, Dict[str, Any]] = {}
    labels_seen: Set[str] = set()

    for key, raw in environ.items():
        if not key.startswith(GROUP_PLUGINS_ENV_PREFIX):
            continue
        suffix = key[len(GROUP_PLUGINS_ENV_PREFIX) :]
        if not suffix:
            continue
        label = suffix.lower()
        if label in labels_seen:
            raise ValueError(
                f"Duplicate bundled venv group {label!r}: multiple env keys map to this "
                f"group id after lowercasing (e.g. {GROUP_PLUGINS_ENV_PREFIX}COMMON and "
                f"{GROUP_PLUGINS_ENV_PREFIX}common)"
            )
        labels_seen.add(label)

        plugins = [p.strip() for p in raw.split(",") if p.strip()]
        if not plugins:
            raise ValueError(
                f"{key} must list at least one plugin (comma-separated); got empty value"
            )
        groups[label] = {"plugins": plugins}

    for key, raw in environ.items():
        if not key.startswith(GROUP_AUX_PRIMARY_ENV_PREFIX):
            continue
        suffix = key[len(GROUP_AUX_PRIMARY_ENV_PREFIX) :]
        if not suffix:
            continue
        label = suffix.lower()
        if label not in groups:
            raise ValueError(
                f"{key} sets auxiliary primary for group {label!r}, but no "
                f"{GROUP_PLUGINS_ENV_PREFIX}<same suffix> group was defined"
            )
        primary = raw.strip()
        if not primary:
            raise ValueError(f"{key} must be a non-empty plugin name")
        existing = groups[label].get("aux_primary_plugin")
        if existing is not None and existing != primary:
            raise ValueError(
                f"Conflicting auxiliary primary for group {label!r}: "
                f"{existing!r} vs {primary!r}"
            )
        groups[label]["aux_primary_plugin"] = primary

    if not groups:
        return None
    return {"groups": groups}


@dataclass(frozen=True)
class BundledVenvGroupPlan:
    """One physical venv build plus member plugins that point at it."""

    label: str
    members: Tuple[str, ...]
    extras: Tuple[str, ...]
    canonical_dir_name: str  # e.g. common-venv (named groups); plugin-bundled for singletons
    # Which member drives auxiliary install variant selection; None = resolver chooses.
    aux_primary_plugin: Optional[str] = None


def _resolve_group_extras(
    label: str,
    group_cfg: Mapping[str, Any],
    members: List[str],
    slim_mode: bool,
) -> List[str]:
    if "extras" in group_cfg and group_cfg["extras"] is not None:
        raw = group_cfg["extras"]
        if not isinstance(raw, list) or not all(isinstance(x, str) for x in raw):
            raise ValueError(
                f'Group "{label}": "extras" must be a list of strings when set'
            )
        return sorted(set(raw))

    if "primary_plugin" in group_cfg and group_cfg["primary_plugin"] is not None:
        primary = group_cfg["primary_plugin"]
        if not isinstance(primary, str):
            raise ValueError(
                f'Group "{label}": "primary_plugin" must be a string when set'
            )
        if primary not in members:
            raise ValueError(
                f'Group "{label}": primary_plugin "{primary}" must be in plugins list'
            )
        return sorted_union_extras([primary], slim_mode)

    return sorted_union_extras(members, slim_mode)


def build_group_plans(
    plugins: List[str],
    groups_config: Optional[Mapping[str, Any]],
    slim_mode: bool,
) -> List[BundledVenvGroupPlan]:
    """
    Build install plans: named groups from config + singletons for unlisted plugins.

    Validates: each plugin appears exactly once; group plugins must be subset of `plugins`.
    """
    plugin_set = set(plugins)
    if len(plugin_set) != len(plugins):
        dupes = [p for p in plugins if plugins.count(p) > 1]
        raise ValueError(f"BUNDLED_VENV_PLUGINS contains duplicates: {set(dupes)}")

    assigned: Set[str] = set()
    plans: List[BundledVenvGroupPlan] = []

    groups_section: Dict[str, Any] = {}
    if groups_config and "groups" in groups_config:
        raw = groups_config["groups"]
        if not isinstance(raw, dict):
            raise ValueError('"groups" must be an object')
        groups_section = dict(raw)

    for label, group_cfg in groups_section.items():
        if not isinstance(group_cfg, dict):
            raise ValueError(f'Group "{label}" must be an object')
        raw_plugins = group_cfg.get("plugins")
        if not raw_plugins:
            raise ValueError(f'Group "{label}": "plugins" is required and non-empty')
        if not isinstance(raw_plugins, list) or not all(
            isinstance(x, str) for x in raw_plugins
        ):
            raise ValueError(f'Group "{label}": "plugins" must be a list of strings')
        members = [p.strip() for p in raw_plugins if p.strip()]
        if not members:
            raise ValueError(f'Group "{label}": "plugins" is empty')

        unknown = [m for m in members if m not in plugin_set]
        if unknown:
            raise ValueError(
                f'Group "{label}" references plugins not in BUNDLED_VENV_PLUGINS: {unknown}'
            )

        overlap = assigned.intersection(members)
        if overlap:
            raise ValueError(
                f"Plugins {sorted(overlap)} appear in more than one group "
                f'(duplicate membership); offending group "{label}"'
            )

        extras = _resolve_group_extras(label, group_cfg, members, slim_mode)
        canonical = f"{label}-venv"
        aux_raw = group_cfg.get("aux_primary_plugin")
        aux_primary: Optional[str] = None
        if aux_raw is not None:
            if not isinstance(aux_raw, str):
                raise ValueError(
                    f'Group "{label}": "aux_primary_plugin" must be a string when set'
                )
            c = aux_raw.strip()
            if c not in members:
                raise ValueError(
                    f'Group "{label}": aux_primary_plugin {c!r} must be in plugins list'
                )
            aux_primary = c
        assigned.update(members)
        plans.append(
            BundledVenvGroupPlan(
                label=label,
                members=tuple(members),
                extras=tuple(extras),
                canonical_dir_name=canonical,
                aux_primary_plugin=aux_primary,
            )
        )

    for p in plugins:
        if p not in assigned:
            ex = sorted_union_extras([p], slim_mode)
            plans.append(
                BundledVenvGroupPlan(
                    label=p,
                    members=(p,),
                    extras=tuple(ex),
                    canonical_dir_name=f"{p}-bundled",
                    aux_primary_plugin=None,
                )
            )

    return plans
