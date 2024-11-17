# fmt: off
from dagster import Definitions, load_assets_from_modules

# Define Assets
""" 
Load all assets from the reviews module so they can be registered in the pipeline
"""

from .assets import census
all_assets = load_assets_from_modules([census])


# Define Resources
from .resources import \
    azure_blob_resource

"""
Create a dictionary mapping resource names to resource configurations
-> Enable assets to access & interact with external storage and database
"""

all_resources = {
    'azure_blob_client': azure_blob_resource
}


# Create Definitions to groups all the resources and assets together
"""
Execute the data processing workflow defined by the assets, using defined resources
"""

defs = Definitions(
    resources=all_resources,
    assets=all_assets
)
