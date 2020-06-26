from common import run

# See https://github.com/snowflakedb/snowflake-sqlalchemy for more details
URL = '' # e.g. snowflake://<user_login_name>:<password>@<account_name>
OPTIONS = {} # e.g. {"connect_args": {"timezone": "America/Los_Angeles"}}
PLATFORM = 'snowflake'

run(URL, OPTIONS, PLATFORM)