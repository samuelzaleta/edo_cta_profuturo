from types import ModuleType
from jaydebeapi import Connection as DBConnection
from sqlalchemy.engine import default, Connection
from sqlalchemy.sql import compiler


class RdbIdentifierPreparer(compiler.IdentifierPreparer):
    def __init__(self, dialect):
        super(RdbIdentifierPreparer, self).__init__(
            dialect,
            initial_quote="",
            final_quote="",
        )


class RdbJayDeBeApiDialect(default.DefaultDialect):
    name = "rdb"
    driver = "jaydebeapi"
    preparer = RdbIdentifierPreparer

    @classmethod
    def dbapi(cls) -> ModuleType:
        import jaydebeapi

        return jaydebeapi

    def initialize(self, connection: Connection) -> None:
        dbconn: DBConnection = connection.connection

        dbconn.jconn.setAutoCommit(False)
