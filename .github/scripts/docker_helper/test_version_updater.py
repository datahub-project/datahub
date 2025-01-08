import pytest
import tempfile
import os
from version_updater import update_pyproject_version, update_init_version

@pytest.fixture
def test_versions():
    return [
        "1!0.0.0+docker.pr4788",
        "0.3.7.7",
        "0.3.8rc1"
    ]

@pytest.mark.parametrize("version", [
    "1!0.0.0+docker.pr4788",
    "0.3.7.7",
    "0.3.8rc1"
])
def test_pyproject_version(version):
    test_content = 'version = "1!0.0.0.dev0"\n'
    
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tf:
        tf.write(test_content)
        temp_path = tf.name
    
    try:
        update_pyproject_version(temp_path, version)
        with open(temp_path) as f:
            result = f.read()
        expected = f'version = "{version}"\n'
        assert result == expected
    finally:
        os.unlink(temp_path)

@pytest.mark.parametrize("version", [
    "1!0.0.0+docker.pr4788",
    "0.3.7.7",
    "0.3.8rc1"
])
def test_init_version(version):
    test_content = '__version__ = "1!0.0.0.dev0"\n'
    
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tf:
        tf.write(test_content)
        temp_path = tf.name
    
    try:
        update_init_version(temp_path, version)
        with open(temp_path) as f:
            result = f.read()
        expected = f'__version__ = "{version}"\n'
        assert result == expected
    finally:
        os.unlink(temp_path)

def test_file_not_found():
    with pytest.raises(FileNotFoundError):
        update_pyproject_version("nonexistent.toml", "1.0.0")
    
    with pytest.raises(FileNotFoundError):
        update_init_version("nonexistent.py", "1.0.0")
