from common import run

# See https://docs.sqlalchemy.org/en/13/dialects/postgresql.html#module-sqlalchemy.dialects.postgresql.psycopg2 for more details
URL = '' # e.g. postgresql+psycopg2://user:password@host:port
OPTIONS = {} # e.g. {"client_encoding": "utf8"}
PLATFORM = 'postgresql'

run(URL, OPTIONS, PLATFORM)