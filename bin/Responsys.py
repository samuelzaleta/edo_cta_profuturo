from profuturo.common import define_extraction, register_time, notify
from profuturo.database import get_postgres_pool, get_bigquery_pool, get_postgres_oci_pool
from profuturo.extraction import extract_terms, _get_spark_session
from profuturo.env import load_env
import requests
import time
import json
import sys
import os


load_env()
postgres_pool = get_postgres_pool()
postgres_oci_pool = get_postgres_oci_pool()
phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])
url = os.getenv("URL_RESPONSYS")
print(url)


def get_token():
    try:
        payload = {"isNonRepudiation": True}
        secret = os.environ.get("JWT_SECRET")  # Ensure you have set the JWT_SECRET environment variable

        if secret is None:
            raise ValueError("JWT_SECRET environment variable is not set")

        # Set expiration time 10 seconds from now
        expiration_time = datetime.utcnow() + timedelta(seconds=10)
        payload['exp'] = expiration_time.timestamp()  # Setting expiration time directly in payload

        # Create the token
        non_repudiation_token = jwt.encode(payload, secret, algorithm='HS256')

        return non_repudiation_token
    except Exception as error:
        return -1


def get_headers():
    non_repudiation_token = get_token()
    if non_repudiation_token != -1:
        return {"Authorization": f"Bearer {non_repudiation_token}"}
    else:
        return {}

with define_extraction(phase, area, postgres_pool, postgres_oci_pool) as (postgres,postgres_oci):
    term = extract_terms(postgres, phase)
    term_id = term["id"]


    with register_time(postgres_pool, phase, term_id, user, area):

        headers = get_headers()  # Get the headers using the get_headers() function

        for i in range(1000):
            response = requests.get(url, headers=headers)  # Pass headers with the request
            print(response)

            if response.status_code == 200:
                content = response.content.decode('utf-8')
                data = json.loads(content)
                if data['data']['statusText'] == 'finalizado':
                    break
                time.sleep(8)
            else:
                print(f"La solicitud no fue exitosa. Código de estado: {response.status_code}")
                break

        time.sleep(40)

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