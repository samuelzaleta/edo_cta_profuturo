from types import ModuleType
from jaydebeapi import Connection as DBConnection
from sqlalchemy.engine import default, Connection


class RdbJayDeBeApiDialect(default.DefaultDialect):
    name = "rdb"
    driver = "jaydebeapi"

    @classmethod
    def dbapi(cls) -> ModuleType:
        import jaydebeapi

        return jaydebeapi

    def initialize(self, connection: Connection) -> None:
        dbconn: DBConnection = connection.connection

        dbconn.jconn.setAutoCommit(False)
