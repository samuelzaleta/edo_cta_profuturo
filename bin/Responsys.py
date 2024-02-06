from profuturo.common import define_extraction, register_time, notify
from profuturo.database import get_postgres_pool, get_bigquery_pool
from profuturo.extraction import extract_terms, _get_spark_session
from profuturo.env import load_env
import requests
import time
import json
import sys
import os


load_env()
postgres_pool = get_postgres_pool()
phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])
url = os.getenv("URL_RESPONSYS")
print(url)

with define_extraction(phase, area, postgres_pool) as (postgres):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    spark = _get_spark_session()
    spark.conf.set("spark.sql.shuffle.partitions", 100)
    spark.conf.set("spark.default.parallelism", 100)

    with register_time(postgres_pool, phase, term_id, user, area):
        for i in range(1000):
            response = requests.get(url)
            print(response)
            # Verifica si la petición fue exitosa
            if response.status_code == 200:
                # Si la petición fue exitosa, puedes acceder al contenido de la respuesta de la siguiente manera:
                content = response.content.decode('utf-8')
                data = json.loads(content)
                if data['data']['statusText'] == 'finalizado':
                    break
                time.sleep(8)
            else:
                # Si la petición no fue exitosa, puedes imprimir el código de estado para obtener más información
                print(f"La solicitud no fue exitosa. Código de estado: {response.status_code}")
                break

        notify(
            postgres,
            "responsys",
            phase,
            area,
            term=term_id,
            message="Se terminaron de enviar la informacion a responsys",
            aprobar=False,
            descarga=False,
        )