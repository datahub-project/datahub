# This import verifies that the dependencies are available.
import pymysql  # noqa: F401

from datahub.ingestion.source.sql.mysql import MySQLSource


class MariaDBSource(MySQLSource):
    def get_platform(self):
        return "mariadb"
