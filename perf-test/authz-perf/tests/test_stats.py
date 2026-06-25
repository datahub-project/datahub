from lib.stats import compute_stats


def test_compute_stats_empty() -> None:
    stats = compute_stats([])
    assert stats.n == 0
    assert stats.max == 0.0


def test_compute_stats_max_not_trimmed() -> None:
    samples = [10.0, 11.0, 12.0, 100.0]
    stats = compute_stats(samples)
    assert stats.max == 100.0
    assert stats.p50 == 11.5
    assert stats.max_to_p50_ratio > 8.0


def test_percentiles() -> None:
    samples = list(range(1, 101))
    stats = compute_stats([float(x) for x in samples])
    assert stats.min == 1.0
    assert stats.max == 100.0
    assert 49 <= stats.p50 <= 51
    assert stats.p95 >= 94
