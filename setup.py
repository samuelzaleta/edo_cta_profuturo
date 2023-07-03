from setuptools import setup

setup(
    name="profuturo",
    version="0.1.0",
    packages=["profuturo"],
    entry_points={
        "sqlalchemy.dialects": [
            "rdb.jaydebeapi = profuturo.rdb:RdbJayDeBeApiDialect",
        ],
    },
)
