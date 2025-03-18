import logging
from hashlib import md5
from typing import List, Optional

from datahub.ingestion.source.confluent_schema_registry import ConfluentSchemaRegistry
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    KafkaSchema,
    SchemaMetadata,
)

logger = logging.getLogger(__name__)


class CustomConfluentSchemaRegistry(ConfluentSchemaRegistry):
    def __init__(self, config, report):
        super().__init__(config, report)

    @classmethod
    def create(cls, config, report):
        return cls(config, report)

    # def _get_subjects_for_topic(self, topic: str) -> Dict[str, Tuple[Optional[str], Optional[str]]]:
    #     """
    #     하나의 토픽에 대해 여러개의 스키마에서 RecordName이 같은 것끼리의 key-value 튜플로 만듬
    #     return : dict(recordName , tuple ( key subjectName , value subjectName ))
    #     dsp 내 subjectName 관련 :
    #         - TopicNameStrategy ( <topic name>-<key/value> ): TopicName = TableName
    #         - RecordNameStrategy : RecordName = {TopicName}.{TableName}_value/key
    #         - TopicRecordNameStrategy ( <topic name>.<record name> ) : RecordName = tableName_value/key
    #     """
    #     subject_pairs = {}
    #     for subject in self.known_schema_registry_subjects:
    #         if subject.startswith(topic + "-") or subject.startswith(topic + "."):
    #             # TopicNameStrategy: topic-key, topic-value
    #             if subject == f"{topic}-key" or subject == f"{topic}-value":
    #                 record_name = topic
    #             # TopicRecordNameStrategy or RecordNameStrategy: topic.RecordName-key, topic.RecordName-value
    #             else:
    #                 # topic.RecordName 를 뽑도록 진행
    #                 record_name = subject.replace("_key", "").replace("_value", "").replace("-key", "").replace("-value", "")
    #
    #             is_key = subject.endswith("-key") or subject.endswith("_key")
    #             is_value = subject.endswith("-value") or subject.endswith("_value")
    #
    #             if record_name not in subject_pairs:
    #                 subject_pairs[record_name] = (None, None)
    #
    #             key_schema, value_schema = subject_pairs[record_name]
    #
    #             if is_key:
    #                 subject_pairs[record_name] = (subject, value_schema)
    #             elif is_value:
    #                 subject_pairs[record_name] = (key_schema, subject)
    #
    #     return subject_pairs

    # 하나의 토픽에 대해 여러개의 subject 를 뽑음
    def _get_subjects_for_topic(
        self, topic: str, is_subject: bool
    ) -> List[Optional[str]]:
        subjects = set()
        if is_subject:
            subjects.add(topic)
        else:
            # TODO : 레지스트리를 계속 돌지 않게끔 변경할 방법 필요
            if self.source_config.topic_patterns.allowed(topic):
                for subject in self.known_schema_registry_subjects:
                    if subject.startswith(topic + "-") or subject.startswith(
                        topic + "."
                    ):
                        record_name = (
                            subject.replace("_key", "")
                            .replace("_value", "")
                            .replace("-key", "")
                            .replace("-value", "")
                        )
                        subjects.add(record_name)
        return list(subjects)

    def _get_subject_for_topic(self, topic: str, is_key_schema: bool) -> Optional[str]:
        subject_key_suffix: str = "-key" if is_key_schema else "-value"
        subject_key_suffix_other: str = (
            "_key" if is_key_schema else "_value"
        )  # dsp 내 RecordNameStrategy Naming 규칙

        # Subject name format when the schema registry subject name strategy is
        #  (a) TopicNameStrategy(default strategy): <topic name>-<key/value>
        #  (b) TopicRecordNameStrategy: <topic name>-<fully-qualified record name>-<key/value>
        #  there's a third case
        #  (c) TopicNameStrategy differing by environment name suffixes.
        #       e.g "a.b.c.d-value" and "a.b.c.d.qa-value"
        #       For such instances, the wrong schema registry entries could picked by the previous logic.
        subject_key: str = topic + subject_key_suffix
        if subject_key in self.source_config.topic_subject_map:
            return self.source_config.topic_subject_map[
                subject_key
            ]  # topic_subject_map 에 있는 subject 명으로 반환
        for subject in self.known_schema_registry_subjects:
            ## a 케이스
            if (
                self.source_config.disable_topic_record_naming_strategy
                and subject == subject_key
            ):
                return subject
            ## b 케이스 / dsp 정규식의 경우 항상 여기서 걸림
            if (
                (not self.source_config.disable_topic_record_naming_strategy)
                and subject.startswith(topic)
                and (
                    subject.endswith(subject_key_suffix)
                    or subject.endswith(subject_key_suffix_other)
                )
            ):
                return subject
        return None

    def get_schema_metadata(
        self, topic: str, platform_urn: str, is_subject: bool
    ) -> List[SchemaMetadata]:
        logger.debug(f"Inside get_schema_metadata {topic} {platform_urn}")

        subjects = self._get_subjects_for_topic(topic, is_subject)
        dataset_metadata_list = []

        for subject in subjects:
            # Process the value schema
            schema, fields = self._get_schema_and_fields(
                topic=subject, is_key_schema=False, is_subject=is_subject
            )  # type: Tuple[Optional[Schema], List[SchemaField]]
            # Process the key schema
            key_schema, key_fields = self._get_schema_and_fields(
                topic=subject, is_key_schema=True, is_subject=is_subject
            )  # type: Tuple[Optional[Schema], List[SchemaField]]

            if schema is not None or key_schema is not None:
                schema_as_string = (schema.schema_str if schema else "") + (
                    key_schema.schema_str if key_schema else ""
                )
                md5_hash = md5(schema_as_string.encode()).hexdigest()

                # key, value 스키마에서 중복 컬럼에 대해서 삭제 ( key 데이터를 유지하기 위해, key 에 존재하면 value 삭제 )
                # fieldPath 꼴 ex) '[version=2.0].[key=True].[type=dm_csprf_customer_tel_l_key].[type=string].sale_corp_cd'
                unique_fields = {
                    field.fieldPath.split(".")[-1]: field for field in key_fields
                }
                for field in fields:
                    column_name = field.fieldPath.split(".")[-1]
                    if column_name not in unique_fields:
                        unique_fields[column_name] = field

                schema_metadata = SchemaMetadata(
                    schemaName=subject
                    if len(subjects) > 1
                    else topic,  # topic or SchemaName => topic or topic.recordName or SchemaName
                    version=0,
                    hash=md5_hash,
                    platform=platform_urn,
                    platformSchema=KafkaSchema(
                        documentSchema=schema.schema_str if schema else "",
                        documentSchemaType=schema.schema_type if schema else None,
                        keySchema=key_schema.schema_str if key_schema else None,
                        keySchemaType=key_schema.schema_type if key_schema else None,
                    ),
                    fields=list(unique_fields.values()),
                )
                dataset_metadata_list.append(schema_metadata)

        if (
            len(subjects) > 1 or not dataset_metadata_list
        ):  # 1:N 일때도 토픽 추출되도록 추가
            dataset_metadata_list.append(
                SchemaMetadata(
                    schemaName=topic,
                    version=0,
                    hash=md5(topic.encode()).hexdigest(),
                    platform=platform_urn,
                    platformSchema=KafkaSchema(
                        documentSchema="",
                        documentSchemaType=None,
                        keySchema=None,
                        keySchemaType=None,
                    ),
                    fields=[],
                )
            )
        return dataset_metadata_list
