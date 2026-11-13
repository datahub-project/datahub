from compare import compare_runs


def _row(
    persona: str,
    operation: str,
    p95: float,
    mode: str = "isolated",
    *,
    metric_key: str | None = None,
    performance_profile: str | None = None,
    expected: int = 200,
) -> dict:
    mk = metric_key or f"{operation}@expect{expected}"
    return {
        "run_id": "r1",
        "persona": persona,
        "operation": operation,
        "metric_key": mk,
        "performance_profile": performance_profile or "authz_allow",
        "cache_phase": "warm",
        "harness": {"execution_mode": mode},
        "stats": {"p50": p95, "p95": p95, "max": p95 * 2},
        "correctness": {"expected_status_code": expected},
    }


def test_compare_regression() -> None:
    baseline = [_row("persona-admin", "getMe", 100.0)]
    candidate = [_row("persona-admin", "getMe", 200.0)]
    code, lines = compare_runs(
        baseline,
        candidate,
        threshold_p95_ratio=1.25,
        threshold_max_ratio=None,
        persona_filter=None,
        operation_filter=None,
        allow_concurrent_compare=False,
    )
    assert code == 1
    assert any("REGRESSION" in line for line in lines)


def test_compare_execution_mode_mismatch() -> None:
    baseline = [_row("p", "getMe", 10.0, "isolated")]
    candidate = [_row("p", "getMe", 10.0, "concurrent")]
    code, lines = compare_runs(
        baseline,
        candidate,
        threshold_p95_ratio=1.25,
        threshold_max_ratio=None,
        persona_filter=None,
        operation_filter=None,
        allow_concurrent_compare=False,
    )
    assert code == 1
    assert "execution_mode mismatch" in lines[0]


def _row_with_deployment(gms_url: str, gms_version: str) -> dict:
    row = _row("persona-admin", "getMe", 100.0)
    row["deployment"] = {
        "gms_url": gms_url,
        "gms_host": "host",
        "gms_version": gms_version,
    }
    return row


def test_compare_deployment_warning() -> None:
    baseline = [_row_with_deployment("http://a:8080", "v0.14.0")]
    candidate = [_row_with_deployment("http://b:8080", "v0.15.0")]
    _, lines = compare_runs(
        baseline,
        candidate,
        threshold_p95_ratio=1.25,
        threshold_max_ratio=None,
        persona_filter=None,
        operation_filter=None,
        allow_concurrent_compare=False,
    )
    assert any("WARN: deployment.gms_url differs" in line for line in lines)


def _row_with_auth(view_enabled: bool, expected: int) -> dict:
    profile = "authz_deny" if expected == 403 else "allow_without_view_auth"
    return {
        **_row(
            "persona-zero-authz",
            "getDomain",
            100.0,
            metric_key=f"getDomain@expect{expected}",
            performance_profile=profile,
            expected=expected,
        ),
        "deployment": {"authorization": {"view_enabled": view_enabled}},
    }


def test_compare_authorization_warning() -> None:
    baseline = [_row_with_auth(True, 403)]
    candidate = [_row_with_auth(False, 200)]
    _, lines = compare_runs(
        baseline,
        candidate,
        threshold_p95_ratio=1.25,
        threshold_max_ratio=None,
        persona_filter=None,
        operation_filter=None,
        allow_concurrent_compare=False,
    )
    assert any(
        "WARN: deployment.authorization.view_enabled differs" in line for line in lines
    )


def test_compare_different_metric_keys_are_not_merged() -> None:
    baseline = [_row_with_auth(True, 403)]
    candidate = [_row_with_auth(False, 200)]
    code, lines = compare_runs(
        baseline,
        candidate,
        threshold_p95_ratio=1.25,
        threshold_max_ratio=None,
        persona_filter=None,
        operation_filter=None,
        allow_concurrent_compare=False,
    )
    assert code == 0
    assert any("no matching metric_key rows" in line for line in lines)
    assert any("baseline-only:" in line for line in lines)
    assert any("candidate-only:" in line for line in lines)
