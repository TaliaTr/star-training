import time

from dagster import Config, MetadataValue
import pandas as pd
import serpapi

from . import constants

serp_api_key = constants.SERP_API_KEY

class Config(Config):
    overwrite: bool = False

def create_metadata(
    df,
    start_time,
    config
):
    """
        Create metadata to return after processing.
    """

    processing_time = time.time() - start_time
    processing_time = MetadataValue.float(
        processing_time
    )

    num_rows = len(df)
    num_rows = MetadataValue.int(
        num_rows
    )

    data = df.to_markdown()
    data = MetadataValue.md(
        data
    )

    overwrite = MetadataValue.bool(
        config.overwrite
    )


    metadata ={
        'Processing time': processing_time,
        'Number of rows': num_rows,
        'Data': data,
        'Overwrite': overwrite
    }

    return metadata


# Define the serp_search function to get data_id
def serp_search(
        lat, lon, q, api_key=serp_api_key
        ) -> dict:
    params = {
        "engine": "google_maps",
        "q": q,
        "ll": f"@{lat},{lon},21z",
        "type": "search",
        "hl": "en",
        "gl": "au",
        "api_key": api_key
    }
    results = serpapi.search(params)
    return results.get("place_results", {})

# Define serp search function to get reviews
def serp_review_search(
        data_id, api_key=serp_api_key
    ) -> pd.DataFrame:
    params = {
        "engine": "google_maps_reviews",
        "data_id": data_id,
        "hl": "en",
        "sort_by": "newestFirst",
        "api_key": api_key
    }

    # SerpAPI request
    try:
        results = serpapi.search(params)
        reviews = results.get("reviews", [])
        
    # Add data_id to review results
        for review in reviews:
            review['data_id'] = data_id
        
        return reviews
    except serpapi.exceptions.HTTPError as e:
    # Handle any HTTP errors
        print(f"HTTPError for data_id {data_id}: {e}")
        return []

