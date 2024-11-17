import time
import geopandas as gpd
import pandas as pd
from dagster import Config, MetadataValue


from . import constants


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
        #'Data': data,
        'Overwrite': overwrite
    }

    return metadata


# Load raw cencus data from local file
def bronze_census_data(
        data_dir: str,
        data_year: int = 2021,
        data_topic: str = 'G01',
        geo_area: str = 'AUST',
        gda_spec: str = 'GDA2020',
        gda_type: str = 'STE'
    ) -> gpd.GeoDataFrame:
    """
    Description
    -----------
    Load geographic bronze (raw) census data from a local file.

    Parameters
    ----------
    - data_dir : str
        Data directory.
    - data_year : int = 2021
        Census data year (2016, 2021, ect.).
    - data_topic : str = 'G01'
        Census data topic id (G01, G40, ect.).
    - geo_area : str = 'AUST'
        Census data geographic area (AUST, QLD, ect.).
    - gda_spec : str = 'GDA2020'
        Geographic digital boundary specification (GDA94, GDA2020, ect.).
    - gda_type : str = 'SA4'
        Type of digital geo bounds to use (LGA, SA2, ect.).

    Returns
    -------
    - gdf : geopandas.GeoDataFrame
        Geopandas dataframe.
    """

    # Set filename and layer
    folder = \
        f'Geopackage_{data_year}_{data_topic}_{geo_area}_{gda_spec}'
    file = \
        f'{data_topic}_{geo_area}_{gda_spec}.gpkg'

    filename = \
        f'{data_dir}/{folder}/{file}'
    layer = \
        f'{data_topic}_{gda_type}_{data_year}_{geo_area}'

    # Load data
    gdf = gpd.read_file(
        filename=filename,
        layer=layer
    )

    return gdf

# Create silver census data from bronze
def silver_census_data(
        gdf: gpd.GeoDataFrame,
        data_year: int = 2021,
        gda_type: str = 'SA4'
    ) -> gpd.GeoDataFrame:
    """
    
    Process geographic bronze (raw) census dataframe into silver (processed) data.
    
    """
    # Define columns to be used
    gdf_columns = [
        {
            'name': f'{gda_type}_NAME_{data_year}',
            'rename': 'Location',
            'type': 'str'
        },
        {
            'name': 'Tot_P_P',
            'rename': 'Population',
            'type': 'int'
        },
        {
            'name': 'geometry',
            'rename': 'geometry',
            'type': 'geometry'
        }
    ]
    
    # Ensure the geometry column is valid
    gdf = gdf[gdf['geometry'].notna()].reset_index(drop=True)

    # Select and rename columns
    columns = [col['name'] for col in gdf_columns]
    gdf = gdf[columns]
    column_names = {col['name']: col['rename'] for col in gdf_columns}
    gdf = gdf.rename(columns=column_names)

    # Convert types
    column_types = {col['rename']: col['type'] for col in gdf_columns}
    gdf = gdf.astype(column_types)

    return gdf

# Create gold census data from silver
def gold_census_data(
        gdf: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
    """

    Process silver census data into gold data by adding centroid information.
    
    """
    # Add centroid column representing the center of each geometry
    gdf['centroid'] = gdf.geometry.centroid

    gdf['longitude'] = gdf['centroid'].apply(lambda x: x.bounds[0])
    gdf['latitude'] = gdf['centroid'].apply(lambda x: x.bounds[1])

    # Return the gold-level data with the centroid added
    return gdf