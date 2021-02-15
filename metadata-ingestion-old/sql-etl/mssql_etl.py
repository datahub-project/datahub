from common import run

# See https://github.com/m32/sqlalchemy-tds for more details
URL = '' # e.g. mssql+pytds://username:password@hostname:port
OPTIONS = {}
PLATFORM = 'mssql'

run(URL, OPTIONS, PLATFORM)