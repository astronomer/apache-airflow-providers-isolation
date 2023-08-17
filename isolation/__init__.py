__version__ = "0.1.0"


# This is needed to allow Airflow to pick up specific metadata fields it needs for certain features.
def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-isolation",  # Required
        "name": "Isolated Operators",  # Required
        "description": "Runtime Operator Isolation in Airflow",  # Required
        "versions": [__version__],  # Required
    }
