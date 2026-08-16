from typing import Dict, List, Set

from hypothesis import HealthCheck, given, settings, strategies as st

from datahub.ingestion.source.snowflake.constants import SemanticViewColumnSubtype
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SemanticViewColumnMetadata,
    SnowflakeSemanticView,
)
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)

_TABLE = "T"

# Deliberately collision-prone: every base name can appear in several casings, so
# a generated schema routinely holds case-only pairs. Real Snowflake fixtures are
# almost all uppercase, which is exactly why hand-written cases keep missing this
# class -- the collision has to be generated on purpose to show up at all.
_BASES = ["col", "amount", "id"]
_CASINGS = [str.lower, str.upper, str.title]

_stored_names = st.lists(
    st.tuples(st.sampled_from(_BASES), st.sampled_from(range(len(_CASINGS)))),
    min_size=1,
    max_size=6,
).map(lambda pairs: sorted({_CASINGS[i](base) for base, i in pairs}))


def _identifiers(
    *, preserve_column_case: bool, convert_urns_to_lowercase: bool
) -> SnowflakeIdentifierBuilder:
    config = SnowflakeV2Config.model_validate(
        {
            "account_id": "test_account",
            "username": "u",
            "password": "p",
            "preserve_column_case": preserve_column_case,
            "convert_urns_to_lowercase": convert_urns_to_lowercase,
        }
    )
    return SnowflakeIdentifierBuilder(
        identifier_config=config, structured_reporter=SnowflakeV2Report()
    )


def _view(stored_names: List[str]) -> SnowflakeSemanticView:
    view = SnowflakeSemanticView(
        name="V", created=None, last_altered=None, comment=None, view_definition=""
    )
    # Tag each column's expression with its own name so the answer identifies
    # which column produced it -- a sibling's answer is otherwise indistinguishable.
    view.column_occurrences = {
        name: [
            SemanticViewColumnMetadata(
                name=name,
                data_type="TEXT",
                comment=None,
                subtype=SemanticViewColumnSubtype.DIMENSION,
                table_name=_TABLE,
                synonyms=[],
                expression=f"EXPR::{name}",
            )
        ]
        for name in stored_names
    }
    return view


_CELLS = [
    (preserve, convert) for preserve in (False, True) for convert in (False, True)
]


@settings(max_examples=200, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(stored_names=_stored_names)
def test_preserved_casing_never_collapses_two_columns(stored_names: List[str]) -> None:
    """With casing preserved, distinct stored names keep distinct emitted paths.

    A collapse here is what silently merges two real columns into one field, so
    one of them disappears from DataHub without any error.
    """
    for convert in (False, True):
        identifiers = _identifiers(
            preserve_column_case=True, convert_urns_to_lowercase=convert
        )
        emitted = [identifiers.logical_dataset_field_path(n) for n in stored_names]
        assert len(set(emitted)) == len(stored_names), (
            f"convert={convert}: {stored_names} collapsed onto {sorted(set(emitted))}"
        )


@settings(max_examples=200, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(stored_names=_stored_names)
def test_every_column_resolves_to_itself_not_a_sibling(
    stored_names: List[str],
) -> None:
    """Looking a column up returns that column's own data, in every config cell.

    Stated against the emitted path rather than the stored name so it holds with
    the flag off too: there, case-only spellings are deliberately one column, so
    either answer is correct precisely because both emit the same path.
    """
    view = _view(stored_names)

    for preserve, convert in _CELLS:
        identifiers = _identifiers(
            preserve_column_case=preserve, convert_urns_to_lowercase=convert
        )

        for name in stored_names:
            want = identifiers.logical_dataset_field_path(name)

            expression = SnowflakeSchemaGenerator._semantic_column_expression(
                view, name, _TABLE
            )
            assert expression is not None, f"{name} resolved to no expression"
            answered = expression.removeprefix("EXPR::")
            assert identifiers.logical_dataset_field_path(answered) == want, (
                f"preserve={preserve} convert={convert}: asked {name!r}, "
                f"got {answered!r}'s expression"
            )

            resolved = view.dimension_name_for_join_key(name, _TABLE)
            assert identifiers.logical_dataset_field_path(resolved) == want, (
                f"preserve={preserve} convert={convert}: join key {name!r} "
                f"resolved to {resolved!r}"
            )


@settings(max_examples=200, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(stored_names=_stored_names)
def test_identity_key_agrees_with_the_emitted_path(stored_names: List[str]) -> None:
    """Two names share an identity key exactly when they share an emitted path.

    These are separate functions answering separate questions, and the indices
    keyed by one are used to decide what gets emitted under the other. When they
    disagree, one metric splits into two entities or two collapse into one --
    which is the shape of every bug this suite has had.
    """
    for preserve, convert in _CELLS:
        identifiers = _identifiers(
            preserve_column_case=preserve, convert_urns_to_lowercase=convert
        )

        by_identity: Dict[str, Set[str]] = {}
        by_emitted: Dict[str, Set[str]] = {}
        for name in stored_names:
            by_identity.setdefault(identifiers.column_identity_key(name), set()).add(
                name
            )
            by_emitted.setdefault(
                identifiers.logical_dataset_field_path(name), set()
            ).add(name)

        assert sorted(by_identity.values(), key=sorted) == sorted(
            by_emitted.values(), key=sorted
        ), (
            f"preserve={preserve} convert={convert}: identity groups "
            f"{sorted(by_identity.values(), key=sorted)} != emitted groups "
            f"{sorted(by_emitted.values(), key=sorted)}"
        )
