from dagster import asset, AssetKey, MaterializeResult, AssetExecutionContext

import pandas as pd
import time
import requests
import urllib.parse

from . import constants, support
from .support import Config
from ..resources import AzureBlobClient, MsSqlClient



@asset(
    compute_kind='Python',
    tags={'domain': 'data', 'tier': 'bronze'}
)
def bronze_reviews(
        azure_blob_client: AzureBlobClient,
        config: Config
    ):
    """

    Initial properties data.

    """

    # Set start time
    start_time = time.time()

    # Download properties parquet file from blob storage
    bronze_reviews_df = azure_blob_client.download_data(
        connection_string=constants.STORAGE_CONNECTION_STRING,
        container_name=constants.PROPERTIES_CONTAINER_NAME,
        blob_name=constants.RAW_PROPERTIES_FILE
    )

    # Process data
    # Create a full address column for bronze_reviews df
    bronze_reviews_df["full_address"] = (
        bronze_reviews_df["address_line_1"] + ", " 
        + bronze_reviews_df["address_line_2"] + ", " 
        + bronze_reviews_df["state"] + " "  
        + bronze_reviews_df["post_code"].astype(str)
    )

    bronze_reviews_df = pd.DataFrame(bronze_reviews_df)

    # Upload bronze data
    azure_blob_client.upload_data(
        connection_string=constants.STORAGE_CONNECTION_STRING,
        container_name=constants.BRONZE_CONTAINER,
        blob_name='/google-reviews/locations.csv',
        df=bronze_reviews_df
    )

    # Metadata to show on Dagster UI
    metadata = support.create_metadata(
        bronze_reviews_df,
        start_time,
        config
        )

    return MaterializeResult(metadata=metadata)


@asset(
    deps=[AssetKey(['bronze_reviews'])],
    compute_kind='Python',
    tags={'domain': 'data', 'tier': 'silver'}
)
def silver_reviews(
        azure_blob_client: AzureBlobClient,
        config: Config
    ):
    """

    Geoencoded & data ID added to initial accommodation properties data.

    """

    # Set start time
    start_time = time.time()

    # Get bronze data
    bronze_reviews_df = azure_blob_client.download_data(
        connection_string=constants.STORAGE_CONNECTION_STRING,
        container_name=constants.BRONZE_CONTAINER,
        blob_name=f'/google-reviews/locations.csv'
    )

    # Create silver_reviews_df as a copy of bronze data
    silver_reviews_df = bronze_reviews_df.copy()


    # Lat & lon to add to silver data
    latitudes = []
    longitudes = []

    # URL & API key
    query_url = constants.QUERY_URL
    geo_api_key = constants.GEO_API_KEY

    # Iterate over each row and make GeoAPI calls
    for address in silver_reviews_df.get("full_address", []):
        if pd.notna(address):  # Proceed only if the address is not NaN
            # Ensure address is a string before passing to quote
            if isinstance(address, str):  # Check if address is already a string
                url_address = urllib.parse.quote(address)
            else:
                url_address = urllib.parse.quote(str(address))  # Convert address to string if needed

            url = query_url.format(url_address, geo_api_key)
            headers = {"Accept": "application/json"}

            try:
                response = requests.get(url, headers=headers).json()
                latitude = response['features'][0]['properties'].get('lat', None)
                longitude = response['features'][0]['properties'].get('lon', None)
            except (IndexError, KeyError):
                latitude, longitude = None, None

            # Append latitude and longitude values to the lists
            latitudes.append(latitude)
            longitudes.append(longitude)

        # Set rate limit
        time.sleep(0.2)


    # Add latitude and longitude columns to the silver data
    silver_reviews_df["latitude"] = latitudes
    silver_reviews_df["longitude"] = longitudes

    # Clean data (change hotel name)
    silver_reviews_df['name_long'] = silver_reviews_df['name_long'].replace(
        "Rydges Townsville Southbank Apartments", "Townsville Southbank Apartments"
    )

    # Apply serp_search to get 'place_results' and 'data_id'
    silver_reviews_df['place_results'] = silver_reviews_df[
        ['latitude', 'longitude', 'name_long']
        ].apply(
        lambda x: support.serp_search(x[0], x[1], x[2]),
        axis=1
        )
    
    silver_reviews_df['data_id'] = silver_reviews_df['place_results'].apply(lambda x: x.get('data_id', ''))

    # Select and reorder columns for the final DataFrame
    columns = [
        'id', 'name_short', 'name_long',
        'client_id', 'email', 'mobile', 'phone',
        'address_line_1', 'address_line_2', 'post_code', 'state', 'timezone', 'full_address',
        'latitude', 'longitude', 'data_id'
    ]
    silver_reviews_df = silver_reviews_df[columns]

    # Upload silver data
    azure_blob_client.upload_data(
        connection_string=constants.STORAGE_CONNECTION_STRING,
        container_name=constants.SILVER_CONTAINER,
        blob_name=f'/google-reviews/internal-google-locations.parquet',
        df=silver_reviews_df
    )

    metadata = support.create_metadata(
        silver_reviews_df,
        start_time,
        config
        )

    return MaterializeResult(metadata=metadata)



@asset(
    deps=[AssetKey(['silver_reviews'])],
    compute_kind='Python',
    tags={'domain': 'data', 'tier': 'gold'}
)
def gold_reviews(
        context: AssetExecutionContext,
        azure_blob_client: AzureBlobClient,
        ms_sql_resource: MsSqlClient,
        config: Config
    ):
    """

    Review dataset.

    """
    # Set start time
    start_time = time.time()

    # Get silver_data
    silver_reviews_df = azure_blob_client.download_data(
        connection_string=constants.STORAGE_CONNECTION_STRING,
        container_name=constants.SILVER_CONTAINER,
        blob_name="/google-reviews/internal-google-locations.parquet"
    )

    # Create reviews_df as a copy of silver data
    reviews_df = silver_reviews_df.copy()

    # Serp API key
    serp_api_key = constants.SERP_API_KEY

    # Add reviews to each data_id
    reviews_df["review_results"] = \
        reviews_df["data_id"].apply(lambda x: support.serp_review_search(x, api_key=serp_api_key)if pd.notna(x) and x else [])
    
    # Create golf_reviews_df
    reviews_results = [x for x in reviews_df["review_results"].values.tolist()]
    gold_reviews_df = pd.concat([pd.DataFrame(x) for x in reviews_results])

    # Set columns and rename
    columns = [
        'data_id', 'review_id',
        'iso_date_of_last_edit', 'rating', 'snippet'
    ]
    gold_reviews_df = gold_reviews_df[columns].copy()
    gold_reviews_df = gold_reviews_df.rename(
        columns={
            'iso_date_of_last_edit': 'update_datetime',
            'snippet': 'text'
        }
    )

    # Clean data
    gold_reviews_df['text'] = gold_reviews_df['text'].fillna('')
    gold_reviews_df['update_datetime'] = \
        pd.to_datetime(gold_reviews_df['update_datetime']).dt.tz_localize(None)
    gold_reviews_df = gold_reviews_df.sort_values(
        by='update_datetime',
        ascending=False
    )

    # Add address info back to gold data
    columns = [
        'data_id', 'name_short', 'name_long',
        'client_id', 'email', 'phone',
        'address_line_1', 'address_line_2', 'post_code', 'state',
        'full_address', 'latitude', 'longitude'
    ]
    gold_reviews_df = pd.merge(
        reviews_df[columns],
        gold_reviews_df,
        how='right',
        on='data_id'
    )
    
    context.log.info(str(gold_reviews_df.dtypes))

    # Upload golden data
    azure_blob_client.upload_data(
        connection_string=constants.STORAGE_CONNECTION_STRING,
        container_name=constants.GOLD_CONTAINER,
        blob_name=f'/google-reviews/internal-google-reviews.parquet',
        df=gold_reviews_df
    )

    # Upload gold data to Public storage
    azure_blob_client.upload_data(
        constants.PUBLIC_STORAGE_CONNECTION_STRING,
        constants.GOLD_CONTAINER,
        f'/google-reviews/internal-google-reviews.parquet',
        gold_reviews_df
    )

    # Insert golden data 
    schema_name = 'reviews'
    table_name = 'fact-reviews'
    connection_string = \
        f'{constants.DATABASE_USERNAME}:{constants.DATABASE_PASSWORD}' + \
        f'@{constants.DATABASE_SERVER}/{constants.DATABASE_NAME}'
    ms_sql_resource.insert_data(
        connection_string,
        table_name,
        schema_name,
        gold_reviews_df
    )

    metadata = support.create_metadata(
        gold_reviews_df,
        start_time,
        config
        )

    return MaterializeResult(metadata=metadata)