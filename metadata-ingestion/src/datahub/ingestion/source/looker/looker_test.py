import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Sequence, Union

import looker_sdk
import requests
from looker_sdk.sdk.api40.models import DashboardBase

failed_dashboad_id_via_rest = {}
failed_dashboad_id_via_sdk = {}


def setup_logging():
    """Set up dual logging to console and file."""
    # Create a logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Create handlers for both file and console
    file_handler = logging.FileHandler("looker_api_logs.log")
    console_handler = logging.StreamHandler()

    # Set logging level for both handlers
    file_handler.setLevel(logging.DEBUG)
    console_handler.setLevel(logging.DEBUG)

    # Create a logging format
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


def get_access_token():
    """Authenticate with Looker API and return the access token."""
    url = f"{os.getenv('LOOKERSDK_BASE_URL')}/api/4.0/login"
    data = {
        "client_id": os.getenv("LOOKERSDK_CLIENT_ID"),
        "client_secret": os.getenv("LOOKERSDK_CLIENT_SECRET"),
    }
    response = requests.post(url, data=data)
    if response.status_code == 200:
        return response.json()["access_token"]
    else:
        logging.error("Failed to authenticate with Looker API")
        logging.error(response.text)
        raise Exception("Authentication failed")


def __fields_mapper(fields: Union[str, List[str]]) -> str:
    """Helper method to turn single string or list of fields into Looker API compatible fields param"""
    return fields if isinstance(fields, str) else ",".join(fields)


def fetch_dashboard_sdk(dashboard_id):
    sdk = looker_sdk.init40()
    logging.info(
        f"---------------------- Fetching Dashboard ID {dashboard_id} Via SDK ----------------------"
    )
    try:
        fields = [
            "id",
            "title",
            "dashboard_elements",
            "dashboard_filters",
            "deleted",
            "hidden",
            "description",
            "folder",
            "user_id",
            "created_at",
            "updated_at",
            "last_updater_id",
            "deleted_at",
            "deleter_id",
        ]
        dashboard = sdk.dashboard(dashboard_id, fields=__fields_mapper(fields))
        logging.info(
            f"Dashboard ID {dashboard_id} details fetched successfully via SDK:"
        )
        logging.info(json.dumps(dashboard, indent=2, default=str))
    except Exception as e:
        logging.error(f"SDK Error for Dashboard ID {dashboard_id}: {e}")
        failed_dashboad_id_via_sdk[dashboard_id] = str(e)
    logging.info(
        f"---------------------- Request Completed {dashboard_id} Via SDK----------------------"
    )


def fetch_dashboard(access_token, dashboard_id):
    """Fetch a dashboard by ID using the Looker API and log with separators."""
    headers = {
        "Authorization": f"token {access_token}",
        "Content-Type": "application/json",
    }
    url = f"{os.getenv('LOOKERSDK_BASE_URL')}/api/4.0/dashboards/{dashboard_id}"
    response = requests.get(url, headers=headers)

    logging.info(
        f"---------------------- Fetching Dashboard ID {dashboard_id} ----------------------"
    )

    if response.status_code == 200:
        logging.info(f"Dashboard ID {dashboard_id} details fetched successfully:")
        logging.info(json.dumps(response.json(), indent=2))
    else:
        logging.error(f"Failed to fetch Dashboard ID {dashboard_id}:")
        value = response.content.decode(encoding="utf-8")
        logging.error(value)
        failed_dashboad_id_via_rest[dashboard_id] = response.text
    logging.info(
        f"---------------------- Request Completed {dashboard_id} ----------------------"
    )


def check_environment_variables():
    """Check if all required environment variables are set and log which are missing."""
    required_vars = [
        "LOOKERSDK_CLIENT_ID",
        "LOOKERSDK_CLIENT_SECRET",
        "LOOKERSDK_BASE_URL",
    ]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        error_message = (
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )
        logging.error(error_message)
        raise EnvironmentError(error_message)


def all_dashboards(fields: Union[str, List[str]]) -> Sequence[DashboardBase]:
    sdk = looker_sdk.init40()
    return sdk.all_dashboards(fields=__fields_mapper(fields))


def fetch_dashboard_details(dashboard_id):
    """Wrapper function to call REST and SDK fetch functions."""
    access_token = get_access_token()
    fetch_dashboard(access_token, dashboard_id)
    time.sleep(0.5)
    fetch_dashboard_sdk(dashboard_id)


def main():
    setup_logging()

    check_environment_variables()

    selected_dashboard_ids = [
        "16651",
        "17435",
        "18347",
        "20756",
        "23242",
        "24753",
        "26597",
        "27111",
        "27760",
        "20611",
        "22974",
        "23970",
        "24882",
        "29439",
        "29673",
        "29746",
        "29862",
        "32222",
        "33057",
    ]

    # selected_dashboard_ids: List[Optional[str]] = []
    dashboards = all_dashboards(fields="id")
    # filtered_dashboards = [
    #     "14740",
    #     "18966",
    #     "20900",
    #     "23479",
    #     "24614",
    #     "29246",
    #     "30699",
    #     "32630",
    #     "33846",
    # ]
    dashboard_ids = [dashboard_base.id for dashboard_base in dashboards]
    logging.info(f"Total Dashboard {len(dashboard_ids)}")
    # for id in dashboard_ids:
    #     if id not in filtered_dashboards:
    #         selected_dashboard_ids.append(id)

    logging.info(f"Seleted Dashboard {len(selected_dashboard_ids)}")

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = [
            executor.submit(fetch_dashboard_details, dashboard_id)
            for dashboard_id in selected_dashboard_ids
        ]
        for future in as_completed(futures):
            try:
                future.result()  # This will raise any exceptions caught during the execution of the worker
            except Exception as e:
                logging.error(f"An error occurred: {e}")

    logging.info(f" Failed dashboard via rest {failed_dashboad_id_via_rest}")
    logging.info(f" Failed dashboard via sdk {failed_dashboad_id_via_sdk}")


if __name__ == "__main__":
    main()
