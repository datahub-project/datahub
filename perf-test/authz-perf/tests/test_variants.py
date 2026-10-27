from lib.variants import RunVariant, parse_variants


def test_parse_variants_default() -> None:
    assert parse_variants([], []) == [RunVariant()]


def test_parse_variants_run_labels() -> None:
    variants = parse_variants(["good", "bad"], [])
    assert len(variants) == 2
    assert variants[0].run_label == "good"


def test_parse_variants_docker_tags() -> None:
    variants = parse_variants([], ["v0.14.0", "v0.15.0"])
    assert variants[1].docker_tag == "v0.15.0"
