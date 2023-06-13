import jaydebeapi

conn = jaydebeapi.connect(
    "oracle.rdb.jdbc.rdbThin.Driver",
    "jdbc:rdbThin://130.40.30.144:1707/mexico$base:cierren",
    {"user": "obadillo", "password": "BADILLO2022"},
    "/opt/profuturo/libs/RDBTHIN.JAR"
)

with conn:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM MOV_AVOL LIMIT 5")

        for row in cur.fetchall():
            for column in row:
                print(column, end=",")
            print()
