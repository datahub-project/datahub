package io.datahubproject.openlineage.converter;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.datajob.DataFlowInfo;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.dataset.ConnectionInstanceDetail;
import io.datahubproject.openlineage.dataset.HdfsPathDataset;
import io.datahubproject.openlineage.dataset.HdfsPlatform;
import io.datahubproject.openlineage.dataset.PathSpec;
import io.datahubproject.openlineage.utils.DatahubUtils;
import io.openlineage.client.OpenLineage;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class OpenLineagePlatformResolver {

  static final String FILE_NAMESPACE = "file";
  static final String SCHEME_SEPARATOR = "://";
  static final String TABLE_PREFIX = "table/";
  private static final Pattern GLUE_ARN_PATTERN =
      Pattern.compile("(?:arn:)?aws:glue:([^:]+):([^:]+)");
  static final Map<String, String> PLATFORM_MAP =
      Stream.of(new String[][] {{"awsathena", "athena"}, {"sqlserver", "mssql"}})
          .collect(Collectors.toMap(data -> data[0], data -> data[1]));

  private OpenLineagePlatformResolver() {}

  static Optional<DatasetUrn> convertOpenlineageDatasetToDatasetUrn(
      OpenLineage.Dataset dataset, DatahubOpenlineageConfig mappingConfig) {

    String namespace = dataset.getNamespace();
    String datasetName = dataset.getName();
    Optional<DatasetUrn> datahubUrn;

    if (dataset.getFacets() != null
        && dataset.getFacets().getSymlinks() != null
        && dataset.getFacets().getSymlinks().getIdentifiers() != null
        && !dataset.getFacets().getSymlinks().getIdentifiers().isEmpty()
        && !mappingConfig.isDisableSymlinkResolution()) {
      String connectionKey = null;
      Optional<DatasetUrn> originalUrn =
          getDatasetUrnFromOlDataset(namespace, datasetName, null, mappingConfig);
      for (OpenLineage.SymlinksDatasetFacetIdentifiers symlink :
          dataset.getFacets().getSymlinks().getIdentifiers()) {
        if ("TABLE".equals(symlink.getType())) {
          // Before OpenLineage 0.17.1 the namespace started with "aws:glue:" and after that it was
          // changed to :arn:aws:glue:"
          if (symlink.getNamespace().startsWith("aws:glue:")
              || symlink.getNamespace().startsWith("arn:aws:glue:")) {
            namespace = "glue";
          } else {
            namespace = mappingConfig.getHivePlatformAlias();
          }
          if (symlink.getName().startsWith(TABLE_PREFIX)) {
            datasetName = symlink.getName().replaceFirst(TABLE_PREFIX, "").replace("/", ".");
          } else {
            datasetName = symlink.getName();
          }
          // Derive the connection-instance map key from the symlink's own namespace before it is
          // flattened above (e.g. the Glue ARN, which "glue" alone can't recover). toConnectionKey
          // dispatches on the namespace protocol — it is not Glue-specific.
          connectionKey = toConnectionKey(symlink.getNamespace());
        }
      }
      Optional<DatasetUrn> symlinkedUrn =
          getDatasetUrnFromOlDataset(namespace, datasetName, connectionKey, mappingConfig);
      if (symlinkedUrn.isPresent() && originalUrn.isPresent()) {
        mappingConfig
            .getUrnAliases()
            .put(originalUrn.get().toString(), symlinkedUrn.get().toString());
      }
      datahubUrn = symlinkedUrn;
    } else {
      datahubUrn = getDatasetUrnFromOlDataset(namespace, datasetName, null, mappingConfig);
    }

    log.debug("Dataset URN: {}, alias_list: {}", datahubUrn, mappingConfig.getUrnAliases());
    // If we have the urn in urn aliases then we should use the alias instead of the original urn
    if (datahubUrn.isPresent()
        && mappingConfig.getUrnAliases().containsKey(datahubUrn.get().toString())) {
      try {
        datahubUrn =
            Optional.of(
                DatasetUrn.createFromString(
                    mappingConfig.getUrnAliases().get(datahubUrn.get().toString())));
        return datahubUrn;
      } catch (URISyntaxException e) {
        log.warn("Failed to create URN from alias: {}", e.getMessage());
        return Optional.empty();
      }
    }

    return datahubUrn;
  }

  static Optional<DatasetUrn> getDatasetUrnFromOlDataset(
      String namespace,
      String datasetName,
      String connectionKeyOverride,
      DatahubOpenlineageConfig mappingConfig) {
    String platform;
    if (mappingConfig.isLowerCaseDatasetUrns()) {
      namespace = namespace.toLowerCase(Locale.ROOT);
      datasetName = datasetName.toLowerCase(Locale.ROOT);
    }

    if (namespace.contains(SCHEME_SEPARATOR) || datasetName.contains(SCHEME_SEPARATOR)) {
      try {
        URI datasetUri;
        if (!namespace.contains(SCHEME_SEPARATOR)) {
          // Older Marquez payloads used a platform-like namespace such as "file.localhost"
          // while retaining the canonical filesystem URI in the dataset name.
          datasetUri = new URI(datasetName);
        } else if (!namespace.endsWith("/") && !datasetName.startsWith("/")) {
          datasetUri = new URI(namespace + "/" + datasetName);
        } else {
          datasetUri = new URI(namespace + datasetName);
        }
        if (PLATFORM_MAP.containsKey(datasetUri.getScheme())) {
          platform = PLATFORM_MAP.get(datasetUri.getScheme());
        } else {
          platform = datasetUri.getScheme();
        }
        if (HdfsPlatform.isFsPlatformPrefix(platform)) {
          datasetName = datasetUri.getPath();
          try {
            HdfsPathDataset hdfsPathDataset = HdfsPathDataset.create(datasetUri, mappingConfig);
            DatasetUrn urn = hdfsPathDataset.urn();
            return Optional.of(urn);
          } catch (InstantiationException e) {
            log.warn(
                "Unable to create urn from namespace: {} and dataset {}.", namespace, datasetName);
            return Optional.empty();
          }
        }
      } catch (URISyntaxException e) {
        log.warn("Unable to create URI from namespace: {} and dataset {}.", namespace, datasetName);
        return Optional.empty();
      }
    } else {
      platform = namespace;
      // Bare-namespace FS datasets (e.g. "file", "dbfs") have no scheme, so they don't flow through
      // HdfsPathDataset where file_partition_regexp is normally applied. Apply the opt-in regexp
      // here too — otherwise it is silently ignored for these platforms. Scoped to FS platforms so
      // non-FS bare namespaces (hive, the symlink-flattened glue) are unaffected.
      if (HdfsPlatform.isFsPlatformPrefix(platform)
          && mappingConfig.getFilePartitionRegexpPattern() != null) {
        datasetName =
            HdfsPathDataset.getRawNameWithoutPartition(
                datasetName, mappingConfig.getFilePartitionRegexpPattern());
      }
    }

    // The connection key drives per-connection platform_instance/env resolution. The caller
    // supplies
    // it for the symlink (Glue) case where `namespace` was flattened; otherwise derive it from the
    // namespace via the same protocol-dispatching utility.
    String connectionKey =
        connectionKeyOverride != null ? connectionKeyOverride : toConnectionKey(namespace);
    // When URNs are lowercased the dataset namespace is too (above), so the lookup key must match.
    // The symlink override is derived from the raw symlink namespace, which skips that lowercasing,
    // so normalize here to keep both paths consistent (matters for mixed-case hosts, e.g. Hive).
    if (connectionKey != null && mappingConfig.isLowerCaseDatasetUrns()) {
      connectionKey = connectionKey.toLowerCase(Locale.ROOT);
    }
    // Resolve the per-connection detail once; both platform_instance and env key off it.
    ConnectionInstanceDetail connectionDetail =
        connectionKey == null ? null : mappingConfig.getConnectionInstanceMap().get(connectionKey);
    if (!mappingConfig.getConnectionInstanceMap().isEmpty()) {
      // Surface every connection-key lookup so a configured `connections` entry that never matches
      // an emitted namespace (trailing slash, omitted port, sqlserver:// vs mssql, case when
      // lowerCaseUrns is off, or an authority-less namespace like hive/bigquery) is diagnosable —
      // otherwise the URN silently falls back to the global platform_instance/env and dangles.
      log.debug(
          "Connection-instance lookup: namespace={} -> key={} -> matched={}",
          namespace,
          connectionKey,
          connectionDetail != null);
    }
    String platformInstance = getPlatformInstance(mappingConfig, platform, connectionDetail);
    FabricType env = getEnv(mappingConfig, platform, connectionDetail);
    DatasetUrn urn = DatahubUtils.createDatasetUrn(platform, platformInstance, datasetName, env);
    return Optional.of(urn);
  }

  static String toConnectionKey(String olNamespace) {
    if (olNamespace == null) {
      return null;
    }
    Matcher glueArn = GLUE_ARN_PATTERN.matcher(olNamespace);
    if (glueArn.find()) {
      return "arn:aws:glue:" + glueArn.group(1) + ":" + glueArn.group(2);
    }
    if (olNamespace.contains(SCHEME_SEPARATOR)) {
      return olNamespace;
    }
    return null;
  }

  static FabricType getEnv(
      DatahubOpenlineageConfig mappingConfig, String platform, ConnectionInstanceDetail detail) {
    FabricType fabricType = mappingConfig.getFabricType();
    if (detail != null && detail.getEnv().isPresent()) {
      // env is validated to a FabricType at config-load time, so it is used directly here.
      return detail.getEnv().get();
    }
    if (mappingConfig.getPathSpecs() != null
        && mappingConfig.getPathSpecs().containsKey(platform)) {
      List<PathSpec> pathSpecs = mappingConfig.getPathSpecs().get(platform);
      for (PathSpec pathSpec : pathSpecs) {
        if (pathSpec.getEnv().isPresent()) {
          try {
            fabricType = FabricType.valueOf(pathSpec.getEnv().get());
            return fabricType;
          } catch (IllegalArgumentException e) {
            log.warn("Invalid environment value: {}", pathSpec.getEnv().get());
          }
        }
      }
    }
    return fabricType;
  }

  static String getPlatformInstance(
      DatahubOpenlineageConfig mappingConfig, String platform, ConnectionInstanceDetail detail) {
    // Cross-platform lineage: resolve the upstream connection's instance first via an explicit
    // connection->instance mapping, so datasets from different accounts/regions/hosts in one job
    // get
    // distinct, correct URNs instead of collapsing to a single instance.
    if (detail != null && detail.getPlatformInstance().isPresent()) {
      return detail.getPlatformInstance().get();
    }
    // Use the platform instance from the path spec if it is present otherwise use the one from the
    // commonDatasetPlatformInstance
    String platformInstance = mappingConfig.getCommonDatasetPlatformInstance();
    if (mappingConfig.getPathSpecs() != null
        && mappingConfig.getPathSpecs().containsKey(platform)) {
      List<PathSpec> pathSpecs = mappingConfig.getPathSpecs().get(platform);
      for (PathSpec pathSpec : pathSpecs) {
        if (pathSpec.getPlatformInstance().isPresent()) {
          return pathSpec.getPlatformInstance().get();
        }
      }
    }
    return platformInstance;
  }

  static String getFlowName(String jobName, String flowName) {
    String[] nameSplit = jobName.split("\\.");
    if (flowName != null) {
      return flowName;
    } else {
      return nameSplit[0];
    }
  }

  static DataFlowUrn getFlowUrn(
      String namespace,
      String jobName,
      String processingEngine,
      URI producer,
      DatahubOpenlineageConfig datahubOpenlineageConfig) {
    return getFlowUrn(
        namespace, jobName, processingEngine, null, producer, datahubOpenlineageConfig);
  }

  static DataFlowUrn getFlowUrn(
      String namespace,
      String jobName,
      String processingEngine,
      String jobTypeIntegration,
      URI producer,
      DatahubOpenlineageConfig datahubOpenlineageConfig) {
    String producerName = null;
    if (producer != null) {
      producerName = producer.toString();
    }

    String orchestrator =
        getOrchestrator(
            processingEngine, jobTypeIntegration, producerName, datahubOpenlineageConfig);
    String flowName = datahubOpenlineageConfig.getPipelineName();
    if (datahubOpenlineageConfig.getPlatformInstance() != null) {
      namespace = datahubOpenlineageConfig.getPlatformInstance();
    }
    return (new DataFlowUrn(orchestrator, getFlowName(jobName, flowName), namespace));
  }

  static DataFlowInfo convertRunEventToDataFlowInfo(OpenLineage.RunEvent event, String flowName) {
    DataFlowInfo dataFlowInfo = new DataFlowInfo();
    dataFlowInfo.setName(getFlowName(event.getJob().getName(), flowName));
    return dataFlowInfo;
  }

  static String getOrchestrator(
      String processingEngine,
      String jobTypeIntegration,
      String producer,
      DatahubOpenlineageConfig datahubOpenlineageConfig) {
    List<String> candidates =
        Arrays.asList(
            datahubOpenlineageConfig.getOrchestrator(),
            jobTypeIntegration,
            processingEngine,
            getProducerOrchestrator(producer));
    return candidates.stream()
        .map(OpenLineagePlatformResolver::normalizePlatformName)
        .filter(Objects::nonNull)
        .findFirst()
        .orElse("unknown");
  }

  static String getProducerOrchestrator(String producer) {
    if (producer == null) {
      return null;
    }
    try {
      URI producerUri = URI.create(producer).normalize();
      if (!"github.com".equalsIgnoreCase(producerUri.getHost())) {
        return null;
      }
      String path = Optional.ofNullable(producerUri.getPath()).orElse("").toLowerCase(Locale.ROOT);
      if (path.equals("/apache/airflow") || path.startsWith("/apache/airflow/")) {
        return "airflow";
      }
      if (path.equals("/trinodb/trino") || path.startsWith("/trinodb/trino/")) {
        return "trino";
      }
      for (String integration : List.of("airflow", "flink", "spark", "trino")) {
        if (path.matches(
            "^/openlineage/openlineage/(?:(?:blob|tree)/[^/]+/)?integration/"
                + integration
                + "/?$")) {
          return integration;
        }
      }
      return null;
    } catch (IllegalArgumentException e) {
      log.debug("Unable to match OpenLineage producer URI");
      return null;
    }
  }

  static String normalizePlatformName(String platformName) {
    if (platformName == null || platformName.isBlank()) {
      return null;
    }
    String normalized = platformName.trim().toLowerCase(Locale.ROOT);
    return PLATFORM_MAP.getOrDefault(normalized, normalized);
  }
}
