# from dagster import AssetSelection, define_asset_job

# update_reviews_job = define_asset_job(
#     name="google_reviews_job",
#     selection=AssetSelection.all() - AssetSelection.assets("bronze_reviews")
#         # bronze data is raw data uploaded from a local file, no updates needed
# )