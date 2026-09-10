from pathlib import Path

from lib.paths import default_fixture_dir
from lib.personas import load_personas_oracle

HARNESS_ROOT = Path(__file__).resolve().parent.parent


def test_personas_oracle_has_seventeen_entries() -> None:
    oracles = load_personas_oracle(default_fixture_dir(HARNESS_ROOT))
    assert len(oracles) == 17
