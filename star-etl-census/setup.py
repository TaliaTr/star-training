from setuptools import find_packages, setup

setup(
    name="census",
    packages=find_packages(exclude=["census_tests"]),
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

