import copy
from enum import Enum
from typing import Any, Optional, List, Dict
import logging
from urllib.parse import urlparse

from airflow.datasets import Dataset as AirflowDataset
from openlineage.client.event_v2 import Dataset as OpenLineageDataset
from datahub_airflow_plugin.entities import Dataset as DatahubDataset

logger = logging.getLogger(__name__)

class DatasetType(Enum):
    AIRFLOW = "airflow"
    OPENLINEAGE = "openlineage"
    DATAHUB = "datahub"
    UNKNOWN = "unknown"

class AirflowDatasetConverter:
    """Airflow Dataset을 DataHub Dataset으로 변환하는 컨버터"""

    @staticmethod
    def check_dataset_type(dataset: Any) -> DatasetType:
        """
        데이터셋의 타입을 확인하는 함수

        Args:
            dataset (Any): 확인할 데이터셋 객체

        Returns:
            DatasetType: 데이터셋의 타입(AIRFLOW, OPENLINEAGE, DATAHUB, UNKNOWN)
        """

        try:
            if isinstance(dataset, AirflowDataset):
                return DatasetType.AIRFLOW
            elif isinstance(dataset, OpenLineageDataset):
                return DatasetType.OPENLINEAGE
            elif isinstance(dataset, DatahubDataset):
                return DatasetType.DATAHUB
            else:
                return DatasetType.UNKNOWN
        except Exception as e:
            logger.error(f"Error checking dataset type: {str(e)}", exc_info=True)
            return DatasetType.UNKNOWN

    @staticmethod
    def convert_to_datahub_dataset(dataset: Any) -> Optional[DatahubDataset]:
        """
        다양한 타입의 데이터셋을 Datahub Dataset으로 변환

        Args:
            dataset (Any): 변환할 데이터셋 객체

        Returns:
            Optional[DatahubDataset]: 변환된 DataHub 데이터셋 또는 None(변환 실패 시)
        """
        try:
            dataset_type = AirflowDatasetConverter.check_dataset_type(dataset)

            if dataset_type == DatasetType.AIRFLOW:
                return AirflowDatasetConverter._convert_airflow_dataset(dataset)
            elif dataset_type == DatasetType.OPENLINEAGE:
                return AirflowDatasetConverter._convert_openlineage_dataset(dataset)
            elif dataset_type == DatasetType.DATAHUB:
                return dataset
            else:
                logger.warning(f"Unsupported dataset type: {type(dataset)}")
                return DatahubDataset(
                    platform="unknown",
                    name=f"unknown_dataset_{id(dataset)}"
                )

        except Exception as e:
            logger.error(f"Error converting dataset: {str(e)}", exc_info=True)
            return DatahubDataset(
                platform="unknown",
                name=f"unknown_dataset_{id(dataset)}"
            )

    @staticmethod
    def _convert_airflow_dataset(dataset: AirflowDataset) -> DatahubDataset:
        """
        Airflow Dataset을 DataHub Dataset으로 변환

        Args:
            dataset (AirflowDataset): 변환할 Airflow 데이터셋

        Returns:
            DatahubDataset: 변환된 DataHub 데이터셋

        Raises:
            Exception: 변환 중 오류 발생 시
        """
        try:
            uri = dataset.uri
            logger.debug(f"Converting Airflow Dataset URI: {uri}")

            platform = "airflow"
            if "://" in uri:
                platform = uri.split("://")[0]

            if platform in ["kudu", "impala", "hive"]:
                platform = "hive"

            hostname = urlparse(uri).hostname
            name = hostname.lower() if hostname is not None else uri

            platform_instance = (
                dataset.extra.get('platform_instance')
                if hasattr(dataset, 'extra') and dataset.extra and isinstance(dataset.extra, dict)
                else None
            )

            return DatahubDataset(
                platform=platform,
                platform_instance=platform_instance,
                name=name
            )
        except Exception as e:
            logger.error(f"Error converting Airflow dataset: {str(e)}", exc_info=True)
            raise

    @staticmethod
    def _convert_openlineage_dataset(dataset: OpenLineageDataset) -> Optional[DatahubDataset]:
        """
        OpenLineage Dataset을 DataHub Dataset으로 변환

        Args:
            dataset (OpenLineageDataset): 변환할 OpenLineage 데이터셋

        Returns:
            Optional[DatahubDataset]: 변환된 DataHub 데이터셋 또는 None(변환 실패 시)
        """
        try:
            # OpenLineage Dataset 내부의 원본 데이터셋 확인
            original_dataset = getattr(dataset, 'source_dataset', None) or getattr(dataset, 'original_dataset', None)

            if original_dataset:
                dataset_type = AirflowDatasetConverter.check_dataset_type(original_dataset)
                if dataset_type == DatasetType.AIRFLOW:
                    return AirflowDatasetConverter._convert_airflow_dataset(original_dataset)

            # 원본이 없거나 다른 타입인 경우 OpenLineage Dataset 자체를 변환
            return DatahubDataset(
                platform=dataset.namespace,
                name=dataset.name
            )
        except Exception as e:
            logger.error(f"Error converting OpenLineage dataset: {str(e)}", exc_info=True)
            return None

    @staticmethod
    def create_urn_mapping_table(datasets):
        """
        platform_instance가 None인 URN을 키로, 원래 URN을 값으로 하는 매핑 테이블을 생성합니다.

        Args:
            datasets (List): 데이터셋 객체 리스트

        Returns:
            Dict: {platform_instance가 None인 URN: 원래 URN} 형태의 매핑 테이블
        """
        urn_mapping_table = {}

        for dataset in datasets:
            temp_dataset = copy.deepcopy(dataset)
            temp_dataset.platform_instance = None

            # 매핑 테이블에 추가
            urn_mapping_table[temp_dataset.urn] = dataset.urn

        return urn_mapping_table

    @staticmethod
    def deduplicate_urns(urns: List[str], urn_mapping_table: Dict[str, str]) -> List[str]:
        """URN 목록에서 중복을 제거하고 고유한 URN 목록을 반환합니다."""
        unique_urns = set()

        for urn in filter(None, urns):  # None이나 빈 문자열 필터링
            # 매핑 테이블에 있으면 매핑된 값 사용, 없으면 원래 URN 사용
            unique_urns.add(urn_mapping_table.get(urn, urn))

        return list(unique_urns)