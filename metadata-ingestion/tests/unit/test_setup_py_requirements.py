"""
Validates that all dependency strings in setup.py are valid PEP 508 requirement specifiers.

Uses the official `packaging.requirements.Requirement` parser to validate all
extras_require entries, catching issues like:
- `package==1.0[extras]` (invalid - extras must come before version)
- Malformed version specifiers
- Invalid package names
"""

import subprocess
import sys
from pathlib import Path

import pytest
from packaging.requirements import InvalidRequirement, Requirement


def test_all_extras_require_are_valid_pep508() -> None:
    """
    Validate all extras_require entries using the official PEP 508 parser.

    Runs setup.py with a mocked setuptools.setup() to capture extras_require,
    then validates each requirement string with packaging.requirements.Requirement.

    Uses a simulated release version to ensure version-pinned dependencies
    are validated (in dev mode, _self_pin is empty and bugs may not manifest).
    """
    script = """\
import sys
sys.path.insert(0, 'src')

# Mock setuptools.setup to capture kwargs
captured = {}
import setuptools
setuptools.setup = lambda **kw: captured.update(kw)

# Read and modify setup.py to use a release version
# (in dev mode _self_pin is empty and bugs like "pkg{_self_pin}[extra]" won't manifest)
from pathlib import Path
setup_content = Path('setup.py').read_text()

# Inject a release version override after package_metadata is populated
setup_content = setup_content.replace(
    '_version: str = package_metadata["__version__"]',
    '_version: str = "1.0.0"  # TEST OVERRIDE for PEP 508 validation'
)

exec(compile(setup_content, 'setup.py', 'exec'))

# Output all requirements as JSON lines
import json
for extra, deps in captured.get('extras_require', {}).items():
    for dep in deps:
        print(json.dumps({'extra': extra, 'dep': dep}))
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent.parent,
    )

    if result.returncode != 0:
        pytest.fail(f"Failed to extract dependencies from setup.py:\n{result.stderr}")

    # Validate each requirement with the official PEP 508 parser
    import json

    invalid = []
    for line in result.stdout.strip().split("\n"):
        if not line:
            continue
        data = json.loads(line)
        extra, dep = data["extra"], data["dep"]
        try:
            Requirement(dep)
        except InvalidRequirement as e:
            invalid.append(f"  [{extra}] {dep}\n    → {e}")

    if invalid:
        pytest.fail(
            f"Found {len(invalid)} invalid PEP 508 requirement(s) in setup.py:\n\n"
            + "\n\n".join(invalid)
            + "\n\nCommon fix: Extras must come BEFORE version specifier.\n"
            + "  Wrong: package==1.0[extra]\n"
            + "  Right: package[extra]==1.0"
        )
