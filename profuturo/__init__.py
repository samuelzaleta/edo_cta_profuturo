from sqlalchemy.dialects import registry

registry.register(
    "rdb.jaydebeapi", "profuturo.dialects.rdb", "RdbJayDeBeApiDialect"
)
