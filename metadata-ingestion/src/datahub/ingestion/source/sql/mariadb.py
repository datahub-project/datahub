from datahub.ingestion.source.sql.mysql import MySQLSource


class MariaDBSource(MySQLSource):
    def get_platform(self):
        return "mariadb"
