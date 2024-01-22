import logging
import sys
from datetime import datetime
from typing import Any, List, Optional

import requests

from datahub.ingestion.source.qlik_cloud.config import (
    QLIK_DATETIME_FORMAT,
    Constant,
    QlikSourceConfig,
)
from datahub.ingestion.source.qlik_cloud.data_classes import (
    App,
    Item,
    QlikDataset,
    SchemaField,
    Space,
)

# Logger instance
logger = logging.getLogger(__name__)


class QlikAPI:
    def __init__(self, config: QlikSourceConfig) -> None:
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.config.api_key}",
                "Content-Type": "application/json",
            }
        )
        self.base_url = f"{self.config.tenant_hostname}/api/v1"
        # Test connection by fetching list of api keys
        logger.info("Trying to connect to {}".format(self.base_url))
        self.session.get(f"{self.base_url}/api-keys").raise_for_status()

    def log_http_error(self, message: str) -> Any:
        logger.warning(message)
        _, e, _ = sys.exc_info()
        if isinstance(e, requests.exceptions.HTTPError):
            logger.warning(f"HTTP status-code = {e.response.status_code}")
        logger.debug(msg=message, exc_info=e)
        return e

    def get_spaces(self) -> List[Space]:
        spaces: List[Space] = []
        try:
            response = self.session.get(f"{self.base_url}/spaces")
            response.raise_for_status()
            for space in response.json()[Constant.DATA]:
                spaces.append(
                    Space(
                        id=space[Constant.ID],
                        name=space[Constant.NAME],
                        description=space[Constant.DESCRIPTION],
                        type=space[Constant.TYPE],
                        owner_id=space[Constant.OWNERID],
                        created_at=datetime.strptime(
                            space[Constant.CREATEDAT], QLIK_DATETIME_FORMAT
                        ),
                        updated_at=datetime.strptime(
                            space[Constant.UPDATEDAT], QLIK_DATETIME_FORMAT
                        ),
                    )
                )
        except Exception:
            self.log_http_error(message="Unable to fetch spaces")
        return spaces

    def _get_dataset(self, dataset_id: str) -> Optional[QlikDataset]:
        try:
            response = self.session.get(f"{self.base_url}/data-sets/{dataset_id}")
            response.raise_for_status()
            dataset = response.json()
            return QlikDataset(
                id=dataset[Constant.ID],
                name=dataset[Constant.NAME],
                qri=dataset[Constant.SECUREQRI],
                description=dataset[Constant.DESCRIPTION],
                space_id=dataset.get(Constant.SPACEID, ""),
                owner_id=dataset[Constant.OWNERID],
                created_at=datetime.strptime(
                    dataset[Constant.CREATEDTIME], QLIK_DATETIME_FORMAT
                ),
                updated_at=datetime.strptime(
                    dataset[Constant.LASTMODIFIEDTIME], QLIK_DATETIME_FORMAT
                ),
                type=dataset[Constant.TYPE],
                size=dataset[Constant.OPERATIONAL].get(Constant.SIZE, 0),
                row_count=dataset[Constant.OPERATIONAL][Constant.ROWCOUNT],
                schema=[
                    SchemaField(
                        name=field[Constant.NAME],
                        data_type=field[Constant.DATATYPE][Constant.TYPE],
                        primary_key=field[Constant.PRIMARYKEY],
                        nullable=field[Constant.NULLABLE],
                    )
                    for field in dataset[Constant.SCHEMA][Constant.DATAFIELDS]
                ],
            )
        except Exception:
            self.log_http_error(message="Unable to fetch items")
        return None

    def get_items(self) -> List[Item]:
        items: List[Item] = []
        try:
            response = self.session.get(f"{self.base_url}/items")
            response.raise_for_status()
            data = response.json()[Constant.DATA]
            for item in data:
                resource_type = item[Constant.RESOURCETYPE]
                resource_attributes = item[Constant.RESOURCEATTRIBUTES]
                if resource_type == Constant.APP:
                    response = self.session.get(f"{self.base_url}/app/")
                    items.append(
                        App(
                            id=resource_attributes[Constant.ID],
                            name=resource_attributes[Constant.NAME],
                            qri=f"qri:app:sense://{resource_attributes[Constant.ID]}",
                            description=resource_attributes[Constant.DESCRIPTION],
                            space_id=resource_attributes[Constant.SPACEID],
                            usage=resource_attributes[Constant.USAGE],
                            owner_id=resource_attributes[Constant.OWNERID],
                            created_at=datetime.strptime(
                                resource_attributes[Constant.CREATEDDATE],
                                QLIK_DATETIME_FORMAT,
                            ),
                            updated_at=datetime.strptime(
                                resource_attributes[Constant.MODIFIEDDATE],
                                QLIK_DATETIME_FORMAT,
                            ),
                        )
                    )
                elif resource_type == Constant.DATASET:
                    dataset = self._get_dataset(dataset_id=item[Constant.RESOURCEID])
                    if dataset:
                        items.append(dataset)

        except Exception:
            self.log_http_error(message="Unable to fetch items")
        return items
