"""Unit tests for _datahub_ol_adapter module.

The bulk of these target ``_sanitize_ol_dataset_name`` and the integration
through ``translate_ol_to_datahub_urn``. The motivating regression is
ING-2018: ``apache-airflow-providers-amazon``'s
``S3ToRedshiftOperator.get_openlineage_facets_on_complete`` constructs the
output Dataset name as
``f"{database}.{self.schema}.{self.table}"`` without a ``None`` guard. When
the customer leaves ``schema=None`` and embeds the schema in ``table=``,
that produces names like ``"sprout.None.seed.stg_mixpanel_load_2563508"``
which become orphan DataHub URNs that never merge with the URNs emitted by
DataHub's native Redshift source.
"""

from __future__ import annotations

import logging

import pytest
from openlineage.client.run import Dataset as OpenLineageDataset

from datahub_airflow_plugin import _datahub_ol_adapter as adapter
from datahub_airflow_plugin._datahub_ol_adapter import (
    _sanitize_ol_dataset_name,
    translate_ol_to_datahub_urn,
)


@pytest.fixture(autouse=True)
def _reset_warning_flag() -> None:
    # The sanitiser uses a per-process ``_warning_logged`` flag to fire the
    # WARNING at most once. Tests must observe a fresh process so a warning
    # actually fires whenever we exercise a path that should warn —
    # otherwise test ordering becomes a silent dependency.
    adapter._warning_logged = False


class TestSanitizeOlDatasetName:
    """Direct tests for the sanitiser."""

    @pytest.mark.parametrize(
        "raw",
        [
            "sprout.seed.stg_mixpanel_load_2563508",
            "stg_mixpanel_load_2563508",
            "events-daily-aws-monoschema-production/2563508/mp_master_event/2024/11/30/",
            "",
        ],
    )
    def test_passthrough_when_no_none_segment(
        self, raw: str, caplog: pytest.LogCaptureFixture
    ) -> None:
        # Sanity: clean names must round-trip unchanged so we never alter
        # well-formed URNs. We also assert the happy path is silent — emitting
        # a WARNING for clean inputs would alarm users for no reason and is a
        # contract we want locked in.
        with caplog.at_level(logging.WARNING, logger="datahub_airflow_plugin"):
            assert _sanitize_ol_dataset_name(raw) == raw
        assert caplog.records == []

    def test_strips_literal_none_in_middle(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        # The exact ING-2018 shape: database.None.schema.table
        with caplog.at_level(logging.WARNING):
            result = _sanitize_ol_dataset_name(
                "sprout.None.seed.stg_mixpanel_load_2563508"
            )
        assert result == "sprout.seed.stg_mixpanel_load_2563508"

        # Use ``getMessage()`` (not ``message``) so we assert against the
        # rendered output — we want to be sure both the original and the
        # sanitized name actually appear in the log line the operator sees.
        rendered = [r.getMessage() for r in caplog.records]
        assert any(
            "sprout.None.seed.stg_mixpanel_load_2563508" in m
            and "sprout.seed.stg_mixpanel_load_2563508" in m
            for m in rendered
        ), rendered

    def test_strips_literal_none_at_start(self) -> None:
        assert (
            _sanitize_ol_dataset_name("None.seed.stg_mixpanel_load_2563508")
            == "seed.stg_mixpanel_load_2563508"
        )

    def test_strips_literal_none_at_end(self) -> None:
        assert _sanitize_ol_dataset_name("sprout.seed.None") == "sprout.seed"

    def test_strips_multiple_none_segments(self) -> None:
        assert (
            _sanitize_ol_dataset_name("None.sprout.None.seed.None.tbl")
            == "sprout.seed.tbl"
        )

    def test_strips_empty_segments(self) -> None:
        # Defensive: leading/trailing/double dots can also occur if the
        # upstream f-string left blanks rather than the literal "None".
        assert _sanitize_ol_dataset_name(".sprout..seed.tbl.") == "sprout.seed.tbl"

    def test_does_not_strip_none_substring_inside_segment(self) -> None:
        # Only standalone "None" segments are stripped — never substrings.
        # A table genuinely named "None_handler" must survive untouched.
        assert (
            _sanitize_ol_dataset_name("db.schema.None_handler")
            == "db.schema.None_handler"
        )
        assert (
            _sanitize_ol_dataset_name("db.schema.no_None_here")
            == "db.schema.no_None_here"
        )

    def test_does_not_strip_lowercase_none(self) -> None:
        # We're targeting the Python ``str(None)`` artefact specifically.
        # Real "none" segments in customer data must round-trip.
        assert _sanitize_ol_dataset_name("db.none.tbl") == "db.none.tbl"

    def test_no_dot_no_change(self) -> None:
        # S3-style names have no dots; the sanitiser must short-circuit.
        s3_key = "events-daily-aws-monoschema-production/2563508/mp_master_event/"
        assert _sanitize_ol_dataset_name(s3_key) is s3_key

    @pytest.mark.parametrize(
        "raw",
        [
            "None.None",
            "None.None.None",
            "None..None",
            ".None.",
            "..",
        ],
    )
    def test_all_none_or_empty_returns_original_with_warning(
        self, raw: str, caplog: pytest.LogCaptureFixture
    ) -> None:
        # Pathological case: filtering would leave nothing, so joining would
        # produce ``""`` and a broken URN like
        # ``urn:li:dataset:(urn:li:dataPlatform:redshift,,PROD)`` that is
        # invisible in the UI and search. We instead keep the original
        # malformed name (so the orphan is at least *findable*) and log a
        # WARNING so the upstream bug is loud.
        with caplog.at_level(logging.WARNING):
            result = _sanitize_ol_dataset_name(raw)

        assert result == raw, (
            "All-None/empty input must round-trip unchanged so we never emit "
            "an empty-name URN."
        )
        rendered = [r.getMessage() for r in caplog.records]
        assert any("only 'None'/empty" in m for m in rendered), rendered

    def test_clean_dotted_name_returns_same_object_no_allocation(self) -> None:
        # The docstring promises that the dotted-but-clean path also returns
        # the original ``name`` reference without allocating a join. This
        # ``is`` check locks that contract — a future refactor that
        # accidentally returns ``".".join(parts)`` would break it.
        clean = "db.schema.tbl"
        assert _sanitize_ol_dataset_name(clean) is clean

    def test_does_not_strip_uppercase_none(self) -> None:
        # Case-exact match on the Python ``str(None)`` artefact: only the
        # exact 4-char string ``"None"`` is stripped. ``"NONE"``, ``"none"``,
        # ``"None_"`` etc. all round-trip unchanged.
        assert _sanitize_ol_dataset_name("db.NONE.tbl") == "db.NONE.tbl"

    def test_bare_none_without_dots_passes_through(self) -> None:
        # Documented design decision: the no-dot fast path is taken first so
        # a single bare ``"None"`` is returned unchanged. We intentionally
        # do not rewrite this — a single-token name has no enclosing
        # ``database.schema.`` shape that the bug under fix can produce, and
        # rewriting it would risk silently losing a real (if implausible)
        # table literally named ``None``.
        assert _sanitize_ol_dataset_name("None") == "None"

    def test_warning_logged_at_most_once_per_process(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        # The WARNING is an alarm, not a stream: it fires on the first
        # buggy name and then stays silent for the lifetime of the worker
        # process. A heavily-affected customer can call the sanitiser
        # thousands of times per scheduler cycle (same name, different
        # names, different categories) and still see just one log line —
        # additional lines would only enumerate the symptom, not surface
        # a new bug. Operators discover the full set of orphans by
        # searching DataHub for ``.None.`` or empty segments.
        with caplog.at_level(logging.WARNING):
            _sanitize_ol_dataset_name("a.None.b")  # fires
            _sanitize_ol_dataset_name("c.None.d")  # suppressed
            _sanitize_ol_dataset_name("None.None")  # suppressed
            for _ in range(50):
                _sanitize_ol_dataset_name("a.None.b")  # all suppressed

        rendered = [r.getMessage() for r in caplog.records]
        assert len(rendered) == 1, rendered
        assert "'a.None.b'" in rendered[0]

    def test_translate_all_none_input_does_not_produce_empty_urn(self) -> None:
        # End-to-end guard: even on the pathological all-None path the
        # produced URN must contain the original name segment, never "" — an
        # empty name produces an unsearchable, untyped dataset entity that is
        # strictly worse than the orphan we were trying to fix.
        urn = translate_ol_to_datahub_urn(
            OpenLineageDataset(namespace="redshift://h:5439", name="None.None"),
            env="PROD",
        )
        assert urn == ("urn:li:dataset:(urn:li:dataPlatform:redshift,None.None,PROD)")
        # And critically: no empty-name URN.
        assert ",,PROD)" not in urn


class TestTranslateOlToDatahubUrn:
    """End-to-end tests through the public translator."""

    @staticmethod
    def _ol(namespace: str, name: str) -> OpenLineageDataset:
        return OpenLineageDataset(namespace=namespace, name=name)

    def test_clean_redshift_name_unchanged(self) -> None:
        urn = translate_ol_to_datahub_urn(
            self._ol(
                "redshift://mycluster.us-east-1:5439",
                "sprout.seed.stg_mixpanel_load_2563508",
            ),
            env="PROD",
        )
        assert urn == (
            "urn:li:dataset:(urn:li:dataPlatform:redshift,"
            "sprout.seed.stg_mixpanel_load_2563508,PROD)"
        )

    def test_ing_2018_repro_sanitises_schema_none(self) -> None:
        # Exact reproducer for the customer URN in ING-2018.
        urn = translate_ol_to_datahub_urn(
            self._ol(
                "redshift://mycluster.us-east-1:5439",
                "sprout.None.seed.stg_mixpanel_load_2563508",
            ),
            env="PROD",
        )
        assert urn == (
            "urn:li:dataset:(urn:li:dataPlatform:redshift,"
            "sprout.seed.stg_mixpanel_load_2563508,PROD)"
        )

    def test_scheme_tweaks_still_applied_after_sanitisation(self) -> None:
        # ``awsathena`` -> ``athena`` mapping must still kick in even when
        # the name needs sanitising.
        urn = translate_ol_to_datahub_urn(
            self._ol("awsathena://us-east-1", "catalog.None.db.tbl"),
            env="PROD",
        )
        assert urn == (
            "urn:li:dataset:(urn:li:dataPlatform:athena,catalog.db.tbl,PROD)"
        )

    def test_s3_path_with_dots_in_filenames_unchanged(self) -> None:
        # S3 keys can contain dots but never the literal "None" segment.
        urn = translate_ol_to_datahub_urn(
            self._ol(
                "s3://events-daily",
                "2563508/mp_master_event/2024.11.30/data.json",
            ),
            env="PROD",
        )
        assert urn == (
            "urn:li:dataset:(urn:li:dataPlatform:s3,"
            "2563508/mp_master_event/2024.11.30/data.json,PROD)"
        )

    def test_custom_env_propagated(self) -> None:
        urn = translate_ol_to_datahub_urn(
            self._ol("redshift://h:5439", "db.None.schema.tbl"),
            env="DEV",
        )
        assert urn.endswith(",DEV)")
        assert ".None." not in urn
