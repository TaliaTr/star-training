# fmt: off
from dagster import Definitions, load_assets_from_modules

# Define Assets
""" 
Load all assets from the reviews module so they can be registered in the pipeline
"""

from .assets import reviews
all_assets = load_assets_from_modules([reviews])


# Define Resources
from .resources import \
    azure_blob_resource, ms_sql_resource

"""
Create a dictionary mapping resource names to resource configurations
-> Enable assets to access & interact with external storage and database
"""

all_resources = {
    'azure_blob_client': azure_blob_resource,
    'ms_sql_resource': ms_sql_resource
}


# Define Jobs
from .jobs import update_reviews_job
all_jobs = [update_reviews_job]

# Define Schedules
from .schedules import update_reviews_schedule
all_schedules = [update_reviews_schedule]

# Create Definitions to groups all the resources and assets together
"""
Execute the data processing workflow defined by the assets, using defined resources
"""

defs = Definitions(
    resources=all_resources,
    assets=all_assets,
    jobs=all_jobs,
    schedules=all_schedules
)
