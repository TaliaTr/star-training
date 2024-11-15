from setuptools import find_packages, setup

setup(
    name="reviews",
    packages=find_packages(exclude=["reviews_tests"]),
    install_requires=[
        "dagster==1.9.*",
        "azure-identity",
        "azure-keyvault-secrets",
        "azure-storage-blob",
        "pandas[parquet]",
        "pyodbc",
        "pyarrow",
        "requests",
        "sqlalchemy"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)

