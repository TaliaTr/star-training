from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient

# API KEYS

## Secrets from Azure Key Vaut
ENV = 'dev'

KEY_VAULT_URL = \
    f'https://kv-star-data-{ENV}-002.vault.azure.net/'

SECRET_CLIENT = SecretClient(
    vault_url=KEY_VAULT_URL,
    credential=DefaultAzureCredential()
)

STORAGE_CONNECTION_STRING = \
    SECRET_CLIENT.get_secret(
        "blob-connection-string"
    ).value

PUBLIC_STORAGE_CONNECTION_STRING = \
    SECRET_CLIENT.get_secret(
        "public-blob-connection-string"
    ).value

# Local census ata directory
DATA_DIR = '/home/azureuser/star-etl-census/census/data'

# Blob container
BRONZE_CONTAINER = "bronze-data"
SILVER_CONTAINER = "silver-data"
GOLD_CONTAINER = "gold-data"

# Initialize BlobServiceClient
BLOB_SERVICE_CLIENT = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)

# # SQL database
# DATABASE_SERVER = '10.10.0.5,1433' # make this secret
# DATABASE_NAME = 'gold-database' # make this secret
# DATABASE_USERNAME = 'StarSQL' # make this secret
# DATABASE_PASSWORD = \
#     SECRET_CLIENT.get_secret(
#         f'vm-star-sql-{ENV}-002'
#     ).value