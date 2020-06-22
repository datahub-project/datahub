from common import run

URL = '' # e.g. mysql+pymysql://username:password@hostname:port
OPTIONS = {} # e.g. {"encoding": "latin1"}
PLATFORM = 'mysql'

run(URL, OPTIONS, PLATFORM)