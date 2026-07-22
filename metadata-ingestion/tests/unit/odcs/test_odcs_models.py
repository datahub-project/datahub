import json
import pathlib
from typing import Any, Dict, Set, Type

import pytest
from pydantic import BaseModel

from datahub.ingestion.source.odcs.odcs_models import (
    KNOWN_UNMAPPED_AUTHDEF_FIELDS,
    KNOWN_UNMAPPED_CONTRACT_FIELDS,
    KNOWN_UNMAPPED_PROPERTY_FIELDS,
    KNOWN_UNMAPPED_QUALITY_FIELDS,
    KNOWN_UNMAPPED_SCHEMA_FIELDS,
    KNOWN_UNMAPPED_SERVER_FIELDS,
    KNOWN_UNMAPPED_TEAM_FIELDS,
    KNOWN_UNMAPPED_TEAM_MEMBER_FIELDS,
    ODCSAuthoritativeDefinition,
    ODCSContract,
    ODCSProperty,
    ODCSQualityRule,
    ODCSSchemaObject,
    ODCSServer,
    ODCSTeam,
    ODCSTeamMember,
)

_SCHEMA_DIR = (
    pathlib.Path(__file__).parents[3] / "src/datahub/ingestion/source/odcs/odcs_schema"
)
_SCHEMA_FILES = ("odcs-v3.0.2.json", "odcs-v3.1.0.json")


def _model_keys(model_cls: Type[BaseModel]) -> Set[str]:
    keys: Set[str] = set()
    for fname, finfo in model_cls.model_fields.items():
        keys.add(fname)
        alias = getattr(finfo, "alias", None)
        if alias:
            keys.add(alias)
    return keys


def _spec_keys(node: Any, defs: Dict[str, Any]) -> Set[str]:
    """Collect declared property keys, resolving $refs and schema combinators."""
    out: Set[str] = set()
    if not isinstance(node, dict):
        return out
    ref = node.get("$ref")
    if isinstance(ref, str) and ref.startswith("#/$defs/"):
        return _spec_keys(defs.get(ref.split("/")[-1], {}), defs)
    out |= set((node.get("properties") or {}).keys())
    for combinator in ("allOf", "anyOf", "oneOf"):
        for sub in node.get(combinator) or []:
            out |= _spec_keys(sub, defs)
    for branch in ("if", "then", "else"):
        if branch in node:
            out |= _spec_keys(node[branch], defs)
    return out


def _level_keys() -> Dict[str, Set[str]]:
    """Union of spec keys per object level across both vendored schemas."""
    union: Dict[str, Set[str]] = {}
    for filename in _SCHEMA_FILES:
        spec = json.loads((_SCHEMA_DIR / filename).read_text())
        defs = spec.get("$defs") or {}
        levels = {
            "contract": _spec_keys(spec, defs),
            "schema_object": _spec_keys(defs["SchemaObject"], defs),
            "property": _spec_keys(defs["SchemaProperty"], defs),
            "quality": _spec_keys(defs["DataQuality"], defs),
            "server": _spec_keys(defs["Server"], defs),
            "authdef": _spec_keys(
                (defs.get("AuthoritativeDefinitions") or {}).get("items", {}), defs
            ),
        }
        if "TeamMember" in defs:
            # v3.1: `team` is an object; members have their own definition.
            levels["team_object"] = _spec_keys(defs["Team"], defs)
            levels["team_member"] = _spec_keys(defs["TeamMember"], defs)
        else:
            # v3.0.x: `team` is an array whose items are the member shape.
            levels["team_member"] = _spec_keys(defs["Team"], defs)
        for server_variant in (defs.get("ServerSource") or {}).values():
            levels["server"] |= _spec_keys(server_variant, defs)
        for name, keys in levels.items():
            union.setdefault(name, set()).update(keys)
    return union


@pytest.mark.parametrize(
    "level,model_cls,known_unmapped",
    [
        ("contract", ODCSContract, KNOWN_UNMAPPED_CONTRACT_FIELDS),
        ("schema_object", ODCSSchemaObject, KNOWN_UNMAPPED_SCHEMA_FIELDS),
        ("property", ODCSProperty, KNOWN_UNMAPPED_PROPERTY_FIELDS),
        ("quality", ODCSQualityRule, KNOWN_UNMAPPED_QUALITY_FIELDS),
        ("server", ODCSServer, KNOWN_UNMAPPED_SERVER_FIELDS),
        ("team_object", ODCSTeam, KNOWN_UNMAPPED_TEAM_FIELDS),
        ("team_member", ODCSTeamMember, KNOWN_UNMAPPED_TEAM_MEMBER_FIELDS),
        ("authdef", ODCSAuthoritativeDefinition, KNOWN_UNMAPPED_AUTHDEF_FIELDS),
    ],
)
def test_models_cover_vendored_spec_keys(
    level: str, model_cls: type, known_unmapped: frozenset
) -> None:
    """Every key the vendored ODCS schemas declare must be either parsed by the
    model or explicitly whitelisted as spec-valid-but-unmapped. A gap here means
    valid contracts would be flagged with 'Unknown ODCS field' warnings — the
    fix is to extend the model or the KNOWN_UNMAPPED_* set, deliberately.
    """
    spec_keys = _level_keys()[level]
    covered = _model_keys(model_cls) | set(known_unmapped)
    missing = spec_keys - covered
    assert not missing, (
        f"Spec keys at level '{level}' not covered by {model_cls.__name__} "
        f"or its KNOWN_UNMAPPED whitelist: {sorted(missing)}"
    )


def test_effective_metric_prefers_canonical_key() -> None:
    assert ODCSQualityRule(metric="nullValues").effective_metric == "nullValues"
    assert ODCSQualityRule(rule="rowCount").effective_metric == "rowCount"
    assert (
        ODCSQualityRule(metric="nullValues", rule="rowCount").effective_metric
        == "nullValues"
    )
    assert ODCSQualityRule().effective_metric is None
    assert ODCSQualityRule(metric="  ").effective_metric is None


def test_used_deprecated_rule_key() -> None:
    assert ODCSQualityRule(rule="rowCount").used_deprecated_rule_key
    assert not ODCSQualityRule(metric="rowCount").used_deprecated_rule_key
    assert not ODCSQualityRule(
        metric="rowCount", rule="rowCount"
    ).used_deprecated_rule_key
    assert not ODCSQualityRule().used_deprecated_rule_key


def test_team_members_accepts_both_spec_shapes() -> None:
    list_form = ODCSContract.model_validate(
        {"id": "c1", "team": [{"username": "alice", "role": "owner"}]}
    )
    object_form = ODCSContract.model_validate(
        {
            "id": "c1",
            "team": {
                "name": "my-team",
                "members": [{"username": "alice", "role": "owner"}],
            },
        }
    )
    for contract in (list_form, object_form):
        members = contract.team_members
        assert len(members) == 1
        assert members[0].username == "alice"
    assert ODCSContract.model_validate({"id": "c1"}).team_members == []
    assert (
        ODCSContract.model_validate(
            {"id": "c1", "team": {"name": "empty-team"}}
        ).team_members
        == []
    )
