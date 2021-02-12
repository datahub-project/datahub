import sys
import os

# See https://stackoverflow.com/a/33515264.
sys.path.append(os.path.join(os.path.dirname(__file__), 'test_helpers'))

pytest_plugins = ["tests.integration.fixtures.sql_fixtures"]
