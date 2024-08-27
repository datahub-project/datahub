import * as faker from 'faker';
import kafkaLogo from '../../../images/kafkalogo.png';
import s3Logo from '../../../images/s3.png';
import snowflakeLogo from '../../../images/snowflakelogo.png';
import bigqueryLogo from '../../../images/bigquerylogo.png';
import { DataJob, DataPlatform, EntityType, OwnershipType, PlatformType } from '../../../types.generated';
import { findUserByUsername } from '../searchResult/userSearchResult';

export const platformLogo = {
    kafka: kafkaLogo,
    s3: s3Logo,
    snowflake: snowflakeLogo,
    bigquery: bigqueryLogo,
};

export const generatePlatform = ({ platform, urn }): DataPlatform => {
    return {
        urn,
        type: EntityType.Dataset,
        name: platform,
        properties: {
            type: PlatformType.Others,
            datasetNameDelimiter: '',
            logoUrl: platformLogo[platform],
            __typename: 'DataPlatformProperties',
        },
        __typename: 'DataPlatform',
    };
};

export const dataJobEntity = (): DataJob => {
    const orchestrator = 'airflow';
    const flowId = `datajob_${faker.company.bsNoun()}_${faker.company.bsNoun()}`;
    const cluster = 'prod';
    const dataFlowURN = `(urn:li:dataFlow:(${orchestrator},${flowId},${cluster})`;
    const description = faker.commerce.productDescription();
    const jobId = `load_all_${faker.company.bsNoun()}_${faker.company.bsNoun()}`;
    const kafkaUser = findUserByUsername('kafka');
    const platform = 'kafka';
    const platformURN = `urn:li:dataPlatform:kafka`;
    const dataPlatform = generatePlatform({ platform, urn: platformURN });

    return {
        urn: `urn:li:dataJob:${dataFlowURN},${jobId})`,
        type: EntityType.DataJob,
        dataFlow: {
            urn: dataFlowURN,
            type: EntityType.DataFlow,
            orchestrator,
            flowId,
            cluster,
            info: {
                name: flowId,
                description,
                project: null,
                externalUrl: 'https://airflow.demo.datahubproject.io/tree?dag_id=datahub_analytics_refresh',
                customProperties: [
                    {
                        key: 'end_date',
                        associatedUrn: `urn:li:dataJob:${dataFlowURN},${jobId})`,
                        value: 'None',
                        __typename: 'CustomPropertiesEntry',
                    },
                    {
                        key: 'orientation',
                        value: "'LR'",
                        associatedUrn: `urn:li:dataJob:${dataFlowURN},${jobId})`,
                        __typename: 'CustomPropertiesEntry',
                    },
                    {
                        key: 'max_active_runs',
                        value: '16',
                        associatedUrn: `urn:li:dataJob:${dataFlowURN},${jobId})`,
                        __typename: 'CustomPropertiesEntry',
                    },
                    {
                        key: 'is_paused_upon_creation',
                        value: 'None',
                        associatedUrn: `urn:li:dataJob:${dataFlowURN},${jobId})`,
                        __typename: 'CustomPropertiesEntry',
                    },
                    {
                        key: 'timezone',
                        value: "'UTC'",
                        associatedUrn: `urn:li:dataJob:${dataFlowURN},${jobId})`,
                        __typename: 'CustomPropertiesEntry',
                    },
                    {
                        key: 'params',
                        value: '{}',
                        associatedUrn: `urn:li:dataJob:${dataFlowURN},${jobId})`,
                        __typename: 'CustomPropertiesEntry',
                    },
                    {
                        key: 'fileloc',
                        value: "'/opt/airflow/dags/repo/airflow/dags/datahub_analytics_refresh.py'",
                        associatedUrn: `urn:li:dataJob:${dataFlowURN},${jobId})`,
                        __typename: 'CustomPropertiesEntry',
                    },
                    {
                        key: 'default_args',
                        value: "{<Encoding.VAR: '__var'>: {'owner': 'harshal', 'depends_on_past': False, 'email': ['harshal@acryl.io'], 'email_on_failure': False, 'execution_timeout': {<Encoding.VAR: '__var'>: 300.0, <Encoding.TYPE: '__type'>: <DagAttributeTypes.TIMEDELTA: 'timedelta'>}}, <Encoding.TYPE: '__type'>: <DagAttributeTypes.DICT: 'dict'>}",
                        associatedUrn: `urn:li:dataJob:${dataFlowURN},${jobId})`,
                        __typename: 'CustomPropertiesEntry',
                    },
                    {
                        key: 'tags',
                        value: 'None',
                        associatedUrn: `urn:li:dataJob:${dataFlowURN},${jobId})`,
                        __typename: 'CustomPropertiesEntry',
                    },
                    {
                        key: '_access_control',
                        value: 'None',
                        associatedUrn: `urn:li:dataJob:${dataFlowURN},${jobId})`,
                        __typename: 'CustomPropertiesEntry',
                    },
                    {
                        key: 'doc_md',
                        value: 'None',
                        associatedUrn: `urn:li:dataJob:${dataFlowURN},${jobId})`,
                        __typename: 'CustomPropertiesEntry',
                    },
                    {
                        key: 'dagrun_timeout',
                        value: 'None',
                        associatedUrn: `urn:li:dataJob:${dataFlowURN},${jobId})`,
                        __typename: 'CustomPropertiesEntry',
                    },
                    {
                        key: '_dag_id',
                        value: "'datahub_analytics_refresh'",
                        associatedUrn: `urn:li:dataJob:${dataFlowURN},${jobId})`,
                        __typename: 'CustomPropertiesEntry',
                    },
                    {
                        key: 'catchup',
                        value: 'False',
                        associatedUrn: `urn:li:dataJob:${dataFlowURN},${jobId})`,
                        __typename: 'CustomPropertiesEntry',
                    },
                    {
                        key: 'schedule_interval',
                        value: "{<Encoding.VAR: '__var'>: 86400.0, <Encoding.TYPE: '__type'>: <DagAttributeTypes.TIMEDELTA: 'timedelta'>}",
                        associatedUrn: `urn:li:dataJob:${dataFlowURN},${jobId})`,
                        __typename: 'CustomPropertiesEntry',
                    },
                    {
                        key: '_default_view',
                        value: 'None',
                        associatedUrn: `urn:li:dataJob:${dataFlowURN},${jobId})`,
                        __typename: 'CustomPropertiesEntry',
                    },
                    {
                        key: '_description',
                        value: "'Refresh snowflake tables for analytics purposes'",
                        associatedUrn: `urn:li:dataJob:${dataFlowURN},${jobId})`,
                        __typename: 'CustomPropertiesEntry',
                    },
                    {
                        key: '_concurrency',
                        value: '16',
                        associatedUrn: `urn:li:dataJob:${dataFlowURN},${jobId})`,
                        __typename: 'CustomPropertiesEntry',
                    },
                    {
                        key: 'tasks',
                        value: "[{'execution_timeout': 300.0, 'ui_fgcolor': '#000', '_outlets': {'datasets': []}, 'ui_color': '#f0ede4', 'email_on_failure': False, 'owner': 'harshal', 'task_id': 'run_data_task', '_inlets': {'auto': False, 'task_ids': [], 'datasets': []}, 'template_fields': ['bash_command', 'env'], 'email': ['harshal@acryl.io'], '_downstream_task_ids': ['split_s3_task'], '_task_type': 'BashOperator', '_task_module': 'airflow.operators.bash_operator', 'bash_command': \"echo 'This is where we might run the backup job'\"}, {'execution_timeout': 300.0, 'ui_fgcolor': '#000', '_outlets': {'datasets': [\"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.all_entities', env='PROD')\", \"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.user_basic_info', env='PROD')\", \"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.user_extra_info', env='PROD')\", \"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.dataset_ownerships', env='PROD')\", \"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.dataset_properties', env='PROD')\", \"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.dataset_schemas', env='PROD')\", \"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.dataset_schema_extras', env='PROD')\", \"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.dataset_tags', env='PROD')\", \"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.dataset_lineages', env='PROD')\", \"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.dataset_statuses', env='PROD')\", \"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.tag_ownerships', env='PROD')\", \"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.tag_properties', env='PROD')\", \"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.dataflow_ownerships', env='PROD')\", \"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.dataflow_info', env='PROD')\", \"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.dataflow_tags', env='PROD')\", \"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.datajob_ownerships', env='PROD')\", \"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.datajob_info', env='PROD')\", \"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.datajob_lineages', env='PROD')\", \"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.datajob_tags', env='PROD')\", \"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.chart_info', env='PROD')\", \"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.dashboard_info', env='PROD')\"]}, 'ui_color': '#fff', 'email_on_failure': False, 'owner': 'harshal', 'task_id': 'split_s3_task', '_inlets': {'auto': False, 'task_ids': [], 'datasets': [\"Dataset(platform='s3', name='datahub-demo-backup.demo.aspects', env='PROD')\"]}, 'template_fields': [], 'email': ['harshal@acryl.io'], '_downstream_task_ids': ['load_all_entities_to_snowflake', 'load_tag_ownerships_to_snowflake', 'load_dataset_tags_to_snowflake', 'load_dataset_properties_to_snowflake', 'load_dataset_schema_extras_to_snowflake', 'load_dataset_schemas_to_snowflake', 'load_datajob_lineages_to_snowflake', 'load_dataflow_ownerships_to_snowflake', 'load_dashboard_info_to_snowflake', 'load_datajob_tags_to_snowflake', 'load_dataset_statuses_to_snowflake', 'load_chart_info_to_snowflake', 'load_user_basic_info_to_snowflake', 'load_user_extra_info_to_snowflake', 'load_datajob_info_to_snowflake', 'load_dataset_lineages_to_snowflake', 'load_dataset_ownerships_to_snowflake', 'load_datajob_ownerships_to_snowflake', 'load_tag_properties_to_snowflake', 'load_dataflow_info_to_snowflake', 'load_dataflow_tags_to_snowflake'], '_task_type': 'S3FileToDirectoryTransform', '_task_module': 'unusual_prefix_436311363ce00ecbd709f747db697044ba2913d4_datahub_analytics_refresh'}, {'execution_timeout': 300.0, 'ui_fgcolor': '#000', '_outlets': {'datasets': []}, 'ui_color': '#e8f7e4', 'email_on_failure': False, 'owner': 'harshal', 'task_id': 'wait_for_load_finish', '_inlets': {'auto': False, 'task_ids': [], 'datasets': []}, 'template_fields': [], 'email': ['harshal@acryl.io'], '_downstream_task_ids': ['update_generated_latest_dataset_owners', 'update_generated_latest_dataset_info', 'update_generated_dataset_platforms'], '_task_type': 'DummyOperator', '_task_module': 'airflow.operators.dummy_operator'}, {'execution_timeout': 300.0, 'ui_fgcolor': '#000', '_outlets': {'datasets': [\"Dataset(platform='snowflake', name='demo_pipeline.public.all_entities', env='PROD')\"]}, 'ui_color': '#fff', 'email_on_failure': False, 'owner': 'harshal', 'task_id': 'load_all_entities_to_snowflake', '_inlets': {'auto': False, 'task_ids': [], 'datasets': [\"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.all_entities', env='PROD')\"]}, 'template_fields': [], 'email': ['harshal@acryl.io'], '_downstream_task_ids': ['wait_for_load_finish'], '_task_type': 'LoadS3IntoSnowflakeOperator', '_task_module': 'unusual_prefix_436311363ce00ecbd709f747db697044ba2913d4_datahub_analytics_refresh'}, {'execution_timeout': 300.0, 'ui_fgcolor': '#000', '_outlets': {'datasets': [\"Dataset(platform='snowflake', name='demo_pipeline.public.user_basic_info', env='PROD')\"]}, 'ui_color': '#fff', 'email_on_failure': False, 'owner': 'harshal', 'task_id': 'load_user_basic_info_to_snowflake', '_inlets': {'auto': False, 'task_ids': [], 'datasets': [\"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.user_basic_info', env='PROD')\"]}, 'template_fields': [], 'email': ['harshal@acryl.io'], '_downstream_task_ids': ['wait_for_load_finish'], '_task_type': 'LoadS3IntoSnowflakeOperator', '_task_module': 'unusual_prefix_436311363ce00ecbd709f747db697044ba2913d4_datahub_analytics_refresh'}, {'execution_timeout': 300.0, 'ui_fgcolor': '#000', '_outlets': {'datasets': [\"Dataset(platform='snowflake', name='demo_pipeline.public.user_extra_info', env='PROD')\"]}, 'ui_color': '#fff', 'email_on_failure': False, 'owner': 'harshal', 'task_id': 'load_user_extra_info_to_snowflake', '_inlets': {'auto': False, 'task_ids': [], 'datasets': [\"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.user_extra_info', env='PROD')\"]}, 'template_fields': [], 'email': ['harshal@acryl.io'], '_downstream_task_ids': ['wait_for_load_finish'], '_task_type': 'LoadS3IntoSnowflakeOperator', '_task_module': 'unusual_prefix_436311363ce00ecbd709f747db697044ba2913d4_datahub_analytics_refresh'}, {'execution_timeout': 300.0, 'ui_fgcolor': '#000', '_outlets': {'datasets': [\"Dataset(platform='snowflake', name='demo_pipeline.public.dataset_ownerships', env='PROD')\"]}, 'ui_color': '#fff', 'email_on_failure': False, 'owner': 'harshal', 'task_id': 'load_dataset_ownerships_to_snowflake', '_inlets': {'auto': False, 'task_ids': [], 'datasets': [\"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.dataset_ownerships', env='PROD')\"]}, 'template_fields': [], 'email': ['harshal@acryl.io'], '_downstream_task_ids': ['wait_for_load_finish'], '_task_type': 'LoadS3IntoSnowflakeOperator', '_task_module': 'unusual_prefix_436311363ce00ecbd709f747db697044ba2913d4_datahub_analytics_refresh'}, {'execution_timeout': 300.0, 'ui_fgcolor': '#000', '_outlets': {'datasets': [\"Dataset(platform='snowflake', name='demo_pipeline.public.dataset_properties', env='PROD')\"]}, 'ui_color': '#fff', 'email_on_failure': False, 'owner': 'harshal', 'task_id': 'load_dataset_properties_to_snowflake', '_inlets': {'auto': False, 'task_ids': [], 'datasets': [\"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.dataset_properties', env='PROD')\"]}, 'template_fields': [], 'email': ['harshal@acryl.io'], '_downstream_task_ids': ['wait_for_load_finish'], '_task_type': 'LoadS3IntoSnowflakeOperator', '_task_module': 'unusual_prefix_436311363ce00ecbd709f747db697044ba2913d4_datahub_analytics_refresh'}, {'execution_timeout': 300.0, 'ui_fgcolor': '#000', '_outlets': {'datasets': [\"Dataset(platform='snowflake', name='demo_pipeline.public.dataset_schemas', env='PROD')\"]}, 'ui_color': '#fff', 'email_on_failure': False, 'owner': 'harshal', 'task_id': 'load_dataset_schemas_to_snowflake', '_inlets': {'auto': False, 'task_ids': [], 'datasets': [\"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.dataset_schemas', env='PROD')\"]}, 'template_fields': [], 'email': ['harshal@acryl.io'], '_downstream_task_ids': ['wait_for_load_finish'], '_task_type': 'LoadS3IntoSnowflakeOperator', '_task_module': 'unusual_prefix_436311363ce00ecbd709f747db697044ba2913d4_datahub_analytics_refresh'}, {'execution_timeout': 300.0, 'ui_fgcolor': '#000', '_outlets': {'datasets': [\"Dataset(platform='snowflake', name='demo_pipeline.public.dataset_schema_extras', env='PROD')\"]}, 'ui_color': '#fff', 'email_on_failure': False, 'owner': 'harshal', 'task_id': 'load_dataset_schema_extras_to_snowflake', '_inlets': {'auto': False, 'task_ids': [], 'datasets': [\"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.dataset_schema_extras', env='PROD')\"]}, 'template_fields': [], 'email': ['harshal@acryl.io'], '_downstream_task_ids': ['wait_for_load_finish'], '_task_type': 'LoadS3IntoSnowflakeOperator', '_task_module': 'unusual_prefix_436311363ce00ecbd709f747db697044ba2913d4_datahub_analytics_refresh'}, {'execution_timeout': 300.0, 'ui_fgcolor': '#000', '_outlets': {'datasets': [\"Dataset(platform='snowflake', name='demo_pipeline.public.dataset_tags', env='PROD')\"]}, 'ui_color': '#fff', 'email_on_failure': False, 'owner': 'harshal', 'task_id': 'load_dataset_tags_to_snowflake', '_inlets': {'auto': False, 'task_ids': [], 'datasets': [\"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.dataset_tags', env='PROD')\"]}, 'template_fields': [], 'email': ['harshal@acryl.io'], '_downstream_task_ids': ['wait_for_load_finish'], '_task_type': 'LoadS3IntoSnowflakeOperator', '_task_module': 'unusual_prefix_436311363ce00ecbd709f747db697044ba2913d4_datahub_analytics_refresh'}, {'execution_timeout': 300.0, 'ui_fgcolor': '#000', '_outlets': {'datasets': [\"Dataset(platform='snowflake', name='demo_pipeline.public.dataset_lineages', env='PROD')\"]}, 'ui_color': '#fff', 'email_on_failure': False, 'owner': 'harshal', 'task_id': 'load_dataset_lineages_to_snowflake', '_inlets': {'auto': False, 'task_ids': [], 'datasets': [\"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.dataset_lineages', env='PROD')\"]}, 'template_fields': [], 'email': ['harshal@acryl.io'], '_downstream_task_ids': ['wait_for_load_finish'], '_task_type': 'LoadS3IntoSnowflakeOperator', '_task_module': 'unusual_prefix_436311363ce00ecbd709f747db697044ba2913d4_datahub_analytics_refresh'}, {'execution_timeout': 300.0, 'ui_fgcolor': '#000', '_outlets': {'datasets': [\"Dataset(platform='snowflake', name='demo_pipeline.public.dataset_statuses', env='PROD')\"]}, 'ui_color': '#fff', 'email_on_failure': False, 'owner': 'harshal', 'task_id': 'load_dataset_statuses_to_snowflake', '_inlets': {'auto': False, 'task_ids': [], 'datasets': [\"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.dataset_statuses', env='PROD')\"]}, 'template_fields': [], 'email': ['harshal@acryl.io'], '_downstream_task_ids': ['wait_for_load_finish'], '_task_type': 'LoadS3IntoSnowflakeOperator', '_task_module': 'unusual_prefix_436311363ce00ecbd709f747db697044ba2913d4_datahub_analytics_refresh'}, {'execution_timeout': 300.0, 'ui_fgcolor': '#000', '_outlets': {'datasets': [\"Dataset(platform='snowflake', name='demo_pipeline.public.tag_ownerships', env='PROD')\"]}, 'ui_color': '#fff', 'email_on_failure': False, 'owner': 'harshal', 'task_id': 'load_tag_ownerships_to_snowflake', '_inlets': {'auto': False, 'task_ids': [], 'datasets': [\"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.tag_ownerships', env='PROD')\"]}, 'template_fields': [], 'email': ['harshal@acryl.io'], '_downstream_task_ids': ['wait_for_load_finish'], '_task_type': 'LoadS3IntoSnowflakeOperator', '_task_module': 'unusual_prefix_436311363ce00ecbd709f747db697044ba2913d4_datahub_analytics_refresh'}, {'execution_timeout': 300.0, 'ui_fgcolor': '#000', '_outlets': {'datasets': [\"Dataset(platform='snowflake', name='demo_pipeline.public.tag_properties', env='PROD')\"]}, 'ui_color': '#fff', 'email_on_failure': False, 'owner': 'harshal', 'task_id': 'load_tag_properties_to_snowflake', '_inlets': {'auto': False, 'task_ids': [], 'datasets': [\"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.tag_properties', env='PROD')\"]}, 'template_fields': [], 'email': ['harshal@acryl.io'], '_downstream_task_ids': ['wait_for_load_finish'], '_task_type': 'LoadS3IntoSnowflakeOperator', '_task_module': 'unusual_prefix_436311363ce00ecbd709f747db697044ba2913d4_datahub_analytics_refresh'}, {'execution_timeout': 300.0, 'ui_fgcolor': '#000', '_outlets': {'datasets': [\"Dataset(platform='snowflake', name='demo_pipeline.public.dataflow_ownerships', env='PROD')\"]}, 'ui_color': '#fff', 'email_on_failure': False, 'owner': 'harshal', 'task_id': 'load_dataflow_ownerships_to_snowflake', '_inlets': {'auto': False, 'task_ids': [], 'datasets': [\"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.dataflow_ownerships', env='PROD')\"]}, 'template_fields': [], 'email': ['harshal@acryl.io'], '_downstream_task_ids': ['wait_for_load_finish'], '_task_type': 'LoadS3IntoSnowflakeOperator', '_task_module': 'unusual_prefix_436311363ce00ecbd709f747db697044ba2913d4_datahub_analytics_refresh'}, {'execution_timeout': 300.0, 'ui_fgcolor': '#000', '_outlets': {'datasets': [\"Dataset(platform='snowflake', name='demo_pipeline.public.dataflow_info', env='PROD')\"]}, 'ui_color': '#fff', 'email_on_failure': False, 'owner': 'harshal', 'task_id': 'load_dataflow_info_to_snowflake', '_inlets': {'auto': False, 'task_ids': [], 'datasets': [\"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.dataflow_info', env='PROD')\"]}, 'template_fields': [], 'email': ['harshal@acryl.io'], '_downstream_task_ids': ['wait_for_load_finish'], '_task_type': 'LoadS3IntoSnowflakeOperator', '_task_module': 'unusual_prefix_436311363ce00ecbd709f747db697044ba2913d4_datahub_analytics_refresh'}, {'execution_timeout': 300.0, 'ui_fgcolor': '#000', '_outlets': {'datasets': [\"Dataset(platform='snowflake', name='demo_pipeline.public.dataflow_tags', env='PROD')\"]}, 'ui_color': '#fff', 'email_on_failure': False, 'owner': 'harshal', 'task_id': 'load_dataflow_tags_to_snowflake', '_inlets': {'auto': False, 'task_ids': [], 'datasets': [\"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.dataflow_tags', env='PROD')\"]}, 'template_fields': [], 'email': ['harshal@acryl.io'], '_downstream_task_ids': ['wait_for_load_finish'], '_task_type': 'LoadS3IntoSnowflakeOperator', '_task_module': 'unusual_prefix_436311363ce00ecbd709f747db697044ba2913d4_datahub_analytics_refresh'}, {'execution_timeout': 300.0, 'ui_fgcolor': '#000', '_outlets': {'datasets': [\"Dataset(platform='snowflake', name='demo_pipeline.public.datajob_ownerships', env='PROD')\"]}, 'ui_color': '#fff', 'email_on_failure': False, 'owner': 'harshal', 'task_id': 'load_datajob_ownerships_to_snowflake', '_inlets': {'auto': False, 'task_ids': [], 'datasets': [\"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.datajob_ownerships', env='PROD')\"]}, 'template_fields': [], 'email': ['harshal@acryl.io'], '_downstream_task_ids': ['wait_for_load_finish'], '_task_type': 'LoadS3IntoSnowflakeOperator', '_task_module': 'unusual_prefix_436311363ce00ecbd709f747db697044ba2913d4_datahub_analytics_refresh'}, {'execution_timeout': 300.0, 'ui_fgcolor': '#000', '_outlets': {'datasets': [\"Dataset(platform='snowflake', name='demo_pipeline.public.datajob_info', env='PROD')\"]}, 'ui_color': '#fff', 'email_on_failure': False, 'owner': 'harshal', 'task_id': 'load_datajob_info_to_snowflake', '_inlets': {'auto': False, 'task_ids': [], 'datasets': [\"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.datajob_info', env='PROD')\"]}, 'template_fields': [], 'email': ['harshal@acryl.io'], '_downstream_task_ids': ['wait_for_load_finish'], '_task_type': 'LoadS3IntoSnowflakeOperator', '_task_module': 'unusual_prefix_436311363ce00ecbd709f747db697044ba2913d4_datahub_analytics_refresh'}, {'execution_timeout': 300.0, 'ui_fgcolor': '#000', '_outlets': {'datasets': [\"Dataset(platform='snowflake', name='demo_pipeline.public.datajob_lineages', env='PROD')\"]}, 'ui_color': '#fff', 'email_on_failure': False, 'owner': 'harshal', 'task_id': 'load_datajob_lineages_to_snowflake', '_inlets': {'auto': False, 'task_ids': [], 'datasets': [\"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.datajob_lineages', env='PROD')\"]}, 'template_fields': [], 'email': ['harshal@acryl.io'], '_downstream_task_ids': ['wait_for_load_finish'], '_task_type': 'LoadS3IntoSnowflakeOperator', '_task_module': 'unusual_prefix_436311363ce00ecbd709f747db697044ba2913d4_datahub_analytics_refresh'}, {'execution_timeout': 300.0, 'ui_fgcolor': '#000', '_outlets': {'datasets': [\"Dataset(platform='snowflake', name='demo_pipeline.public.datajob_tags', env='PROD')\"]}, 'ui_color': '#fff', 'email_on_failure': False, 'owner': 'harshal', 'task_id': 'load_datajob_tags_to_snowflake', '_inlets': {'auto': False, 'task_ids': [], 'datasets': [\"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.datajob_tags', env='PROD')\"]}, 'template_fields': [], 'email': ['harshal@acryl.io'], '_downstream_task_ids': ['wait_for_load_finish'], '_task_type': 'LoadS3IntoSnowflakeOperator', '_task_module': 'unusual_prefix_436311363ce00ecbd709f747db697044ba2913d4_datahub_analytics_refresh'}, {'execution_timeout': 300.0, 'ui_fgcolor': '#000', '_outlets': {'datasets': [\"Dataset(platform='snowflake', name='demo_pipeline.public.chart_info', env='PROD')\"]}, 'ui_color': '#fff', 'email_on_failure': False, 'owner': 'harshal', 'task_id': 'load_chart_info_to_snowflake', '_inlets': {'auto': False, 'task_ids': [], 'datasets': [\"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.chart_info', env='PROD')\"]}, 'template_fields': [], 'email': ['harshal@acryl.io'], '_downstream_task_ids': ['wait_for_load_finish'], '_task_type': 'LoadS3IntoSnowflakeOperator', '_task_module': 'unusual_prefix_436311363ce00ecbd709f747db697044ba2913d4_datahub_analytics_refresh'}, {'execution_timeout': 300.0, 'ui_fgcolor': '#000', '_outlets': {'datasets': [\"Dataset(platform='snowflake', name='demo_pipeline.public.dashboard_info', env='PROD')\"]}, 'ui_color': '#fff', 'email_on_failure': False, 'owner': 'harshal', 'task_id': 'load_dashboard_info_to_snowflake', '_inlets': {'auto': False, 'task_ids': [], 'datasets': [\"Dataset(platform='s3', name='datahubproject-demo-pipelines.entity_aspect_splits.dashboard_info', env='PROD')\"]}, 'template_fields': [], 'email': ['harshal@acryl.io'], '_downstream_task_ids': ['wait_for_load_finish'], '_task_type': 'LoadS3IntoSnowflakeOperator', '_task_module': 'unusual_prefix_436311363ce00ecbd709f747db697044ba2913d4_datahub_analytics_refresh'}, {'execution_timeout': 300.0, 'ui_fgcolor': '#000', '_outlets': {'datasets': [\"Dataset(platform='snowflake', name='demo_pipeline.public.generated_dataset_platforms', env='PROD')\"]}, 'ui_color': '#ededed', 'email_on_failure': False, 'owner': 'harshal', 'task_id': 'update_generated_dataset_platforms', '_inlets': {'auto': False, 'task_ids': [], 'datasets': [\"Dataset(platform='snowflake', name='demo_pipeline.public.all_entities', env='PROD')\"]}, 'template_fields': ['sql'], 'email': ['harshal@acryl.io'], '_downstream_task_ids': ['update_generated_latest_dataset_owners', 'update_generated_latest_dataset_info'], '_task_type': 'SnowflakeOperator', '_task_module': 'airflow.providers.snowflake.operators.snowflake', 'sql': \"\\nCREATE OR REPLACE TABLE generated_dataset_platforms AS (\\n    SELECT urn, split(split(urn, ',')[0], ':')[6]::string as platform\\n    FROM all_entities\\n    WHERE all_entities.entity = 'dataset'\\n);\\n        \"}, {'execution_timeout': 300.0, 'ui_fgcolor': '#000', '_outlets': {'datasets': [\"Dataset(platform='snowflake', name='demo_pipeline.public.generated_latest_dataset_owners', env='PROD')\"]}, 'ui_color': '#ededed', 'email_on_failure': False, 'owner': 'harshal', 'task_id': 'update_generated_latest_dataset_owners', '_inlets': {'auto': False, 'task_ids': [], 'datasets': [\"Dataset(platform='snowflake', name='demo_pipeline.public.all_entities', env='PROD')\", \"Dataset(platform='snowflake', name='demo_pipeline.public.dataset_ownerships', env='PROD')\", \"Dataset(platform='snowflake', name='demo_pipeline.public.generated_dataset_platforms', env='PROD')\"]}, 'template_fields': ['sql'], 'email': ['harshal@acryl.io'], '_downstream_task_ids': [], '_task_type': 'SnowflakeOperator', '_task_module': 'airflow.providers.snowflake.operators.snowflake', 'sql': \"\\nCREATE OR REPLACE TABLE generated_latest_dataset_owners AS (\\n    WITH latest_dataset_owners AS (SELECT * FROM dataset_ownerships WHERE version = 0)\\n    SELECT all_entities.urn, generated_dataset_platforms.platform, metadata:owners as owners, ARRAY_SIZE(COALESCE(metadata:owners, array_construct())) as owner_count\\n    FROM all_entities\\n    LEFT JOIN latest_dataset_owners ON all_entities.urn = latest_dataset_owners.urn\\n    LEFT JOIN generated_dataset_platforms ON all_entities.urn = generated_dataset_platforms.urn\\n    WHERE all_entities.entity = 'dataset'\\n);\\n        \"}, {'execution_timeout': 300.0, 'ui_fgcolor': '#000', '_outlets': {'datasets': [\"Dataset(platform='snowflake', name='demo_pipeline.public.generated_latest_dataset_info', env='PROD')\"]}, 'ui_color': '#ededed', 'email_on_failure': False, 'owner': 'harshal', 'task_id': 'update_generated_latest_dataset_info', '_inlets': {'auto': False, 'task_ids': [], 'datasets': [\"Dataset(platform='snowflake', name='demo_pipeline.public.all_entities', env='PROD')\", \"Dataset(platform='snowflake', name='demo_pipeline.public.dataset_properties', env='PROD')\", \"Dataset(platform='snowflake', name='demo_pipeline.public.dataset_tags', env='PROD')\", \"Dataset(platform='snowflake', name='demo_pipeline.public.dataset_lineages', env='PROD')\", \"Dataset(platform='snowflake', name='demo_pipeline.public.generated_dataset_platforms', env='PROD')\"]}, 'template_fields': ['sql'], 'email': ['harshal@acryl.io'], '_downstream_task_ids': [], '_task_type': 'SnowflakeOperator', '_task_module': 'airflow.providers.snowflake.operators.snowflake', 'sql': \"\\nCREATE OR REPLACE TABLE generated_latest_dataset_info AS (\\n    WITH\\n        latest_dataset_info AS (SELECT * FROM dataset_properties WHERE version = 0),\\n        latest_dataset_tags AS (SELECT * FROM dataset_tags WHERE version = 0),\\n        latest_dataset_lineages AS (SELECT * FROM dataset_lineages WHERE version = 0)\\n    SELECT\\n        all_entities.urn,\\n        generated_dataset_platforms.platform,\\n        latest_dataset_info.metadata:description::string as description,\\n        COALESCE(IS_NULL_VALUE(latest_dataset_info.metadata:description), TRUE) as is_missing_docs,\\n        latest_dataset_info.metadata:customProperties as properties,\\n        ARRAY_CAT(COALESCE(latest_dataset_tags.metadata:tags, array_construct()), COALESCE(latest_dataset_info.metadata:tags, array_construct())) as tags,\\n        latest_dataset_lineages.metadata:upstreams as upstreams,\\n        COALESCE(ARRAY_SIZE(latest_dataset_lineages.metadata:upstreams), 0) as upstream_count,\\n        COALESCE(ARRAY_SIZE(latest_dataset_lineages.metadata:upstreams), 0) > 0 as has_upstreams\\n    FROM all_entities\\n    LEFT JOIN latest_dataset_info ON all_entities.urn = latest_dataset_info.urn\\n    LEFT JOIN latest_dataset_tags ON all_entities.urn = latest_dataset_tags.urn\\n    LEFT JOIN latest_dataset_lineages ON all_entities.urn = latest_dataset_lineages.urn\\n    LEFT JOIN generated_dataset_platforms ON all_entities.urn = generated_dataset_platforms.urn\\n    WHERE all_entities.entity = 'dataset'\\n);\\n        \"}]",
                        associatedUrn: `urn:li:dataJob:${dataFlowURN},${jobId})`,
                        __typename: 'CustomPropertiesEntry',
                    },
                    {
                        key: 'start_date',
                        value: '1619913600.0',
                        associatedUrn: `urn:li:dataJob:${dataFlowURN},${jobId})`,
                        __typename: 'CustomPropertiesEntry',
                    },
                ],
                __typename: 'DataFlowInfo',
            },
            editableProperties: null,
            ownership: {
                owners: [],
                lastModified: { time: 1620224528712, __typename: 'AuditStamp' },
                __typename: 'Ownership',
            },
            platform: dataPlatform,
            __typename: 'DataFlow',
        },
        jobId,
        ownership: {
            owners: [
                {
                    owner: kafkaUser,
                    type: OwnershipType.Developer,
                    __typename: 'Owner',
                    associatedUrn: `urn:li:dataJob:${dataFlowURN},${jobId})`,
                },
            ],
            lastModified: { time: 1620079975489, __typename: 'AuditStamp' },
            __typename: 'Ownership',
        },
        inputOutput: {
            inputDatasets: [],
            outputDatasets: [],
            __typename: 'DataJobInputOutput',
        },
        editableProperties: null,
        info: { name: jobId, description: null, __typename: 'DataJobInfo' },
        globalTags: { tags: [], __typename: 'GlobalTags' },
        __typename: 'DataJob',
    };
};
