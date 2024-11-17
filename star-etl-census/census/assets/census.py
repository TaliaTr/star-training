from dagster import asset, AssetKey, MaterializeResult, AssetExecutionContext

import pandas as pd
import time
import requests
import urllib.parse

from . import constants, support
from .support import Config
from ..resources import AzureBlobClient



@asset(
    compute_kind='Python',
    tags={'domain': 'data', 'tier': 'bronze'}
)
def bronze_census(
        azure_blob_client: AzureBlobClient,
        config: Config
    ):
    """

    Raw geographic bronze census data.

    """

    # Set start time
    start_time = time.time()

    # Get raw census data
    bronze_census_df = support.bronze_census_data(
        constants.DATA_DIR,
        2021,
        'G01',
        'AUST',
        'GDA2020',
        'STE'
        )
    
    # Upload bronze data
    azure_blob_client.upload_data(
        connection_string=constants.STORAGE_CONNECTION_STRING,
        container_name=constants.BRONZE_CONTAINER,
        blob_name='geo-census/census-raw.geoparquet',
        df=bronze_census_df
    )

    # # Metadata to show on Dagster UI
    # metadata = support.create_metadata(
    #     bronze_census_df,
    #     start_time,
    #     config
    #     )

    return MaterializeResult()


@asset(
    deps=[AssetKey(['bronze_census'])],
    compute_kind='Python',
    tags={'domain': 'data', 'tier': 'silver'}
)
def silver_census(
        azure_blob_client: AzureBlobClient,
        config: Config
    ):
    """

    Processed geographic census data.

    """

    # # Set start time
    # start_time = time.time()

    # Get bronze data
    bronze_census_df = azure_blob_client.download_data(
        connection_string=constants.STORAGE_CONNECTION_STRING,
        container_name=constants.BRONZE_CONTAINER,
        blob_name=f'geo-census/census-raw.geoparquet'
    )

    # Create silver census data
    silver_census_df = support.silver_census_data(
        bronze_census_df,
        2021,
        'STE'
    )

    # Upload silver data
    azure_blob_client.upload_data(
        connection_string=constants.STORAGE_CONNECTION_STRING,
        container_name=constants.SILVER_CONTAINER,
        blob_name=f'geo-census/silver-census.geoparquet',
        df=silver_census_df
    )

    # metadata = support.create_metadata(
    #     silver_census_df,
    #     start_time,
    #     config
    #     )

    return MaterializeResult()


@asset(
    deps=[AssetKey(['silver_census'])],
    compute_kind='Python',
    tags={'domain': 'data', 'tier': 'gold'}
)
def gold_census(
        context: AssetExecutionContext,
        azure_blob_client: AzureBlobClient,
        config: Config
    ):
    """

    Geo data with centroid, lat, and lon.

    """
    # # Set start time
    # start_time = time.time()

    # Get silver_data
    silver_census_df = azure_blob_client.download_data(
        connection_string=constants.STORAGE_CONNECTION_STRING,
        container_name=constants.SILVER_CONTAINER,
        blob_name="geo-census/silver-census.geoparquet"
    )

    # Create gold data
    gold_census_df = support.gold_census_data(silver_census_df)

    # Upload gold data
    azure_blob_client.upload_data(
        connection_string=constants.STORAGE_CONNECTION_STRING,
        container_name=constants.GOLD_CONTAINER,
        blob_name=f'geo-census/gold-census.geoparquet',
        df=gold_census_df
    )

    # Upload gold data to Public storage
    azure_blob_client.upload_data(
        constants.PUBLIC_STORAGE_CONNECTION_STRING,
        constants.GOLD_CONTAINER,
        f'geo-census/gold-census.geoparquet',
        gold_census_df
    )

    # metadata = support.create_metadata(
    #     gold_census_df,
    #     start_time,
    #     config
    #     )

    return MaterializeResult()