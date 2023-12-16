from dotenv import load_dotenv
from google.cloud import secretmanager
import os


def load_env():
    load_dotenv()
    load_secrets()


def load_secrets():
    if "GCP_PROJECT" not in os.environ:
        raise Exception("GCP_PROJECT not defined. Can not load the secrets")

    client = secretmanager.SecretManagerServiceClient()

    secrets = client.list_secrets(parent=f'projects/{os.getenv("GCP_PROJECT")}')
    for secret in secrets:
        secret_id = secret.name.split('/').pop()
        response = client.access_secret_version(name=f"{secret.name}/versions/latest")

        os.environ[secret_id] = response.payload.data.decode("utf-8")
    print("Loaded secrets from Secret Manager")
