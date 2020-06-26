from common import run

# See https://github.com/dropbox/PyHive for more details
URL = '' # e.g. hive://username:password@hostname:port
OPTIONS = {} # e.g. {"connect_args": {"configuration": {"hive.exec.reducers.max": "123"}}
PLATFORM = 'hive'

run(URL, OPTIONS, PLATFORM)