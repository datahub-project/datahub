package io.datahubproject.openlineage.converter;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Edge;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datajob.DataFlowInfo;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.domain.Domains;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaMetadata;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.dataset.DatahubJob;
import io.openlineage.client.OpenLineage;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class OpenLineageToDataHub {

  public static final String FILE_NAMESPACE = OpenLineagePlatformResolver.FILE_NAMESPACE;
  public static final String SCHEME_SEPARATOR = OpenLineagePlatformResolver.SCHEME_SEPARATOR;
  public static final String URN_LI_CORPUSER = OpenLineageMappingUtils.URN_LI_CORPUSER;
  public static final String URN_LI_CORPUSER_DATAHUB =
      OpenLineageMappingUtils.URN_LI_CORPUSER_DATAHUB;
  public static final String URN_LI_DATA_PROCESS_INSTANCE =
      OpenLineageRunMapper.URN_LI_DATA_PROCESS_INSTANCE;
  public static final String URN_LI_ASSERTION = OpenLineageRunMapper.URN_LI_ASSERTION;
  public static final String ASSERTION_ENTITY_TYPE = OpenLineageRunMapper.ASSERTION_ENTITY_TYPE;
  public static final String PROCESSING_ENGINE_KEY = OpenLineageJobMapper.PROCESSING_ENGINE_KEY;
  public static final String PROCESSING_ENGINE_VERSION_KEY =
      OpenLineageJobMapper.PROCESSING_ENGINE_VERSION_KEY;
  public static final String OPENLINEAGE_ADAPTER_VERSION_KEY =
      OpenLineageJobMapper.OPENLINEAGE_ADAPTER_VERSION_KEY;
  public static final String JOB_ID_KEY = OpenLineageJobMapper.JOB_ID_KEY;
  public static final String JOB_DESCRIPTION_KEY = OpenLineageJobMapper.JOB_DESCRIPTION_KEY;
  public static final String JOB_GROUP_KEY = OpenLineageJobMapper.JOB_GROUP_KEY;
  public static final String JOB_CALL_SITE_KEY = OpenLineageJobMapper.JOB_CALL_SITE_KEY;
  public static final String SPARK_VERSION_KEY = OpenLineageJobMapper.SPARK_VERSION_KEY;
  public static final String OPENLINEAGE_SPARK_VERSION_KEY =
      OpenLineageJobMapper.OPENLINEAGE_SPARK_VERSION_KEY;
  public static final String SPARK_LOGICAL_PLAN_KEY = OpenLineageJobMapper.SPARK_LOGICAL_PLAN_KEY;
  public static final String MERGE_INTO_COMMAND_PATTERN =
      OpenLineageJobMapper.MERGE_INTO_COMMAND_PATTERN;
  public static final String MERGE_INTO_SQL_PATTERN = OpenLineageJobMapper.MERGE_INTO_SQL_PATTERN;
  public static final String TABLE_PREFIX = OpenLineagePlatformResolver.TABLE_PREFIX;
  public static final String WAREHOUSE_PATH_PATTERN = OpenLineageJobMapper.WAREHOUSE_PATH_PATTERN;
  public static final String DB_SUFFIX = OpenLineageJobMapper.DB_SUFFIX;

  public static final Map<String, String> PLATFORM_MAP = OpenLineagePlatformResolver.PLATFORM_MAP;

  private OpenLineageToDataHub() {}

  public static Optional<DatasetUrn> convertOpenlineageDatasetToDatasetUrn(
      OpenLineage.Dataset dataset, DatahubOpenlineageConfig mappingConfig) {
    return OpenLineagePlatformResolver.convertOpenlineageDatasetToDatasetUrn(
        dataset, mappingConfig);
  }

  public static String toConnectionKey(String olNamespace) {
    return OpenLineagePlatformResolver.toConnectionKey(olNamespace);
  }

  public static GlobalTags generateTags(List<String> tags) {
    return OpenLineageMappingUtils.generateTags(tags);
  }

  public static Domains generateDomains(List<String> domains) {
    return OpenLineageMappingUtils.generateDomains(domains);
  }

  public static Urn dataPlatformInstanceUrn(String platform, String instance) {
    return OpenLineageMappingUtils.dataPlatformInstanceUrn(platform, instance);
  }

  public static DatahubJob convertJobEventToJob(
      OpenLineage.JobEvent event, DatahubOpenlineageConfig datahubConf)
      throws IOException, URISyntaxException {
    return OpenLineageJobMapper.convertJobEventToJob(event, datahubConf);
  }

  public static List<MetadataChangeProposal> convertDatasetEventToMcps(
      OpenLineage.DatasetEvent event, DatahubOpenlineageConfig datahubConf) throws IOException {
    return OpenLineageDatasetMapper.convertDatasetEventToMcps(event, datahubConf);
  }

  public static List<MetadataChangeProposal> convertDatasetToMcps(
      OpenLineage.Dataset dataset, DatahubOpenlineageConfig datahubConf) throws IOException {
    return OpenLineageDatasetMapper.convertDatasetToMcps(dataset, datahubConf);
  }

  public static DatasetProperties getDatasetProperties(OpenLineage.Dataset dataset) {
    return OpenLineageDatasetMapper.getDatasetProperties(dataset);
  }

  public static DatahubJob convertRunEventToJob(
      OpenLineage.RunEvent event, DatahubOpenlineageConfig datahubConf)
      throws IOException, URISyntaxException {
    return OpenLineageJobMapper.convertRunEventToJob(event, datahubConf);
  }

  public static Edge createEdge(Urn urn, ZonedDateTime eventTime) {
    return OpenLineageMappingUtils.createEdge(urn, eventTime);
  }

  public static AuditStamp createAuditStamp(ZonedDateTime eventTime) {
    return OpenLineageMappingUtils.createAuditStamp(eventTime);
  }

  public static String getFlowName(String jobName, String flowName) {
    return OpenLineagePlatformResolver.getFlowName(jobName, flowName);
  }

  public static DataFlowUrn getFlowUrn(
      String jobNamespace,
      String jobName,
      String processingEngine,
      URI producer,
      DatahubOpenlineageConfig datahubConf) {
    return OpenLineagePlatformResolver.getFlowUrn(
        jobNamespace, jobName, processingEngine, producer, datahubConf);
  }

  public static DataFlowUrn getFlowUrn(
      String jobNamespace,
      String jobName,
      String processingEngine,
      String jobTypeIntegration,
      URI producer,
      DatahubOpenlineageConfig datahubConf) {
    return OpenLineagePlatformResolver.getFlowUrn(
        jobNamespace, jobName, processingEngine, jobTypeIntegration, producer, datahubConf);
  }

  public static DataFlowInfo convertRunEventToDataFlowInfo(
      OpenLineage.RunEvent event, String flowName) {
    return OpenLineagePlatformResolver.convertRunEventToDataFlowInfo(event, flowName);
  }

  public static SchemaFieldDataType.Type convertOlFieldTypeToDHFieldType(String type) {
    return OpenLineageDatasetMapper.convertOlFieldTypeToDHFieldType(type);
  }

  public static SchemaMetadata getSchemaMetadata(
      OpenLineage.Dataset dataset, DatahubOpenlineageConfig mappingConfig) {
    return OpenLineageDatasetMapper.getSchemaMetadata(dataset, mappingConfig);
  }
}
