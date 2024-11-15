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

##  GeoAPI Key
QUERY_URL = "https://api.geoapify.com/v1/geocode/search?text={}&apiKey={}"
GEOAPI_SECRET_NAME = 'geo-apify-key-talia'
GEO_API_KEY = SECRET_CLIENT.get_secret(GEOAPI_SECRET_NAME).value

## Serp API
SERP_SECRET_NAME = 'serp-api-key-talia'
SERP_API_KEY = SECRET_CLIENT.get_secret(SERP_SECRET_NAME).value

# Blob container
PROPERTIES_CONTAINER_NAME = "properties-reviews"
BRONZE_CONTAINER = "bronze-data"
SILVER_CONTAINER = "silver-data"
GOLD_CONTAINER = "gold-data"
RAW_PROPERTIES_FILE = "accommodation-dim-properties.parquet"

# Initialize BlobServiceClient
BLOB_SERVICE_CLIENT = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
CONTAINER_CLIENT = BLOB_SERVICE_CLIENT.get_container_client(PROPERTIES_CONTAINER_NAME)

# SQL database
DATABASE_SERVER = '10.10.0.5,1433' # make this secret
DATABASE_NAME = 'gold-database' # make this secret
DATABASE_USERNAME = 'StarSQL' # make this secret
DATABASE_PASSWORD = \
    SECRET_CLIENT.get_secret(
        f'vm-star-sql-{ENV}-002'
    ).value