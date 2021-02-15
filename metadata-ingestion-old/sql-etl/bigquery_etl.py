from common import run

# See https://github.com/mxmzdlv/pybigquery/ for more details
URL = '' # e.g. bigquery://project_id
OPTIONS = {} # e.g. {"credentials_path": "/path/to/keyfile.json"}
PLATFORM = 'bigquery'

run(URL, OPTIONS, PLATFORM)