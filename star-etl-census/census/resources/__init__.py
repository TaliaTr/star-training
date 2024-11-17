from azure.storage.blob import BlobClient

from io import BytesIO
import pandas as pd
from sqlalchemy import create_engine
import geopandas as gpd

from dagster import ConfigurableResource


class AzureBlobClient(ConfigurableResource):

    def upload_data(
            self,
            connection_string: str,
            container_name: str,
            blob_name: str,
            df: pd.DataFrame
        ) -> dict:

        # Create blob client
        blob_client = BlobClient.from_connection_string(
            conn_str=connection_string,
            container_name=container_name, 
            blob_name=blob_name
        )

        # Put dataframe into byte stream
        upload_byte_stream = BytesIO()
        file_type = blob_name.split('.')[-1]
        if file_type == 'csv':
            df.to_csv(
                upload_byte_stream,
                index=False
            )
        elif file_type == 'parquet':
            df.to_parquet(
                upload_byte_stream,
                index=False
            )
        elif file_type == 'geoparquet':
            df.to_parquet(
                upload_byte_stream,
                index=False
            )
        else:
            pass
            #RaiseException
        upload_byte_stream.seek(0)

        # Upload byte stream into blob
        blob_client.upload_blob(
            upload_byte_stream,
            overwrite=True
        )

        # Get size metadata
        blob_properties = \
            blob_client.get_blob_properties()

        return blob_properties

    def download_data(
            self,
            connection_string: str,
            container_name: str,
            blob_name: str
        ) -> pd.DataFrame:

        # Create blob client
        blob_client = BlobClient.from_connection_string(
            conn_str=connection_string,
            container_name=container_name,
            blob_name=blob_name
        )

        # Download blob into memory stream
        download_byte_stream = BytesIO()
        blob_client.download_blob().download_to_stream(
            download_byte_stream
        )
        download_byte_stream.seek(0)

        # Read memory stream into dataframe
        file_type = blob_name.split('.')[-1]
        if file_type == 'csv':
            df = pd.read_csv(
                download_byte_stream
            )
        elif file_type == 'parquet':
            df = pd.read_parquet(
                download_byte_stream
            )
        elif file_type == 'geoparquet':
            df = gpd.read_parquet(
                download_byte_stream
            )
        else:
            pass
            #RaiseException

        return df

azure_blob_resource = AzureBlobClient()

# class MsSqlClient(ConfigurableResource):
#     """

#     Client resource for accessing MS SQL Database.

#     """

#     def insert_data(
#             self,
#             connection_string: str,
#             table_name: str,
#             schema_name: str,
#             df: pd.DataFrame
#         ) -> None:

#         # Create SQLAlchemy engine
#         engine = create_engine(
#             f'mssql+pyodbc://{connection_string}'
#             f'?driver=ODBC+Driver+18+for+SQL+Server'
#             '&TrustServerCertificate=yes'
#         )

#         # Insert DataFrame into SQL Server table
#         df.to_sql(
#             table_name,
#             schema=schema_name,
#             con=engine,
#             if_exists='replace',
#             index=False
#         )

# ms_sql_resource = MsSqlClient()