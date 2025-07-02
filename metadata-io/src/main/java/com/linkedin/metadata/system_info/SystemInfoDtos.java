package com.linkedin.metadata.system_info;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Data;

public class SystemInfoDtos {
  public static final String GMS_COMPONENT_NAME = "GMS";
  public static final String MAE_COMPONENT_NAME = "MAE Consumer";
  public static final String MCE_COMPONENT_NAME = "MCE Consumer";
  public static final String GMS_COMPONENT_KEY = "gms";
  public static final String MAE_COMPONENT_KEY = "maeConsumer";
  public static final String MCE_COMPONENT_KEY = "mceConsumer";

  private SystemInfoDtos() {}

  public enum ComponentStatus {
    AVAILABLE,
    UNAVAILABLE,
    ERROR
  }

  @Data
  @Builder
  public static class CompleteSystemInfo {
    private SpringComponentsInfo springComponents;
    private StorageInfo storage;
    private KubernetesInfo kubernetes;
    private KafkaInfo kafka;
    private SystemPropertiesInfo properties;
  }

  @Data
  @Builder
  public static class SystemPropertiesInfo {
    private Map<String, PropertyInfo> properties;
    private List<PropertySourceInfo> propertySources;
    private int totalProperties;
    private int redactedProperties;
  }

  @Data
  @Builder
  public static class PropertySourceInfo {
    private String name;
    private String type;
    private int propertyCount;
  }

  @Data
  @Builder
  public static class SystemInfo {
    private LocalDateTime timestamp;
    private SpringComponentsInfo springComponents;
    private KubernetesInfo kubernetesInfo;
    private StorageInfo storageInfo;
    private KafkaInfo kafkaInfo;
  }

  @Data
  @Builder
  public static class SpringComponentsInfo {
    private ComponentInfo gms;
    private ComponentInfo maeConsumer;
    private ComponentInfo mceConsumer;
  }

  @Data
  @Builder
  public static class ComponentInfo {
    private String name;
    private ComponentStatus status;
    private String version;
    private Map<String, Object> properties;
    private String errorMessage;
  }

  @Data
  @Builder
  public static class KubernetesInfo {
    private boolean runningInKubernetes;
    private String namespace;
    private List<DeploymentInfo> deployments;
    private List<ServiceInfo> services;
    private List<ConfigMapInfo> configMaps;
    private String clusterVersion;
    private HelmInfo helmInfo;
    private String errorMessage;
  }

  @Data
  @Builder
  public static class HelmInfo {
    private boolean installed;
    private String releaseName;
    private String chartName;
    private String chartVersion;
    private String appVersion;
    private String status;
    private String namespace;
    private Map<String, Object> values;
    private String notes;
    private LocalDateTime lastDeployed;
    private String errorMessage;
  }

  @Data
  @Builder
  public static class DeploymentInfo {
    private String name;
    private int replicas;
    private int availableReplicas;
    private Map<String, String> labels;
    private String image;
    private LocalDateTime createdAt;
  }

  @Data
  @Builder
  public static class ServiceInfo {
    private String name;
    private String type;
    private String clusterIP;
    private List<Integer> ports;
    private Map<String, String> selector;
  }

  @Data
  @Builder
  public static class ConfigMapInfo {
    private String name;
    private Map<String, String> data;
  }

  @Data
  @Builder
  public static class StorageInfo {
    private StorageSystemInfo mysql;
    private StorageSystemInfo postgres;
    private StorageSystemInfo elasticsearch;
    private StorageSystemInfo opensearch;
    private StorageSystemInfo neo4j;
    private StorageSystemInfo cassandra;
  }

  @Data
  @Builder
  public static class StorageSystemInfo {
    private String type;
    private boolean available;
    private String version;
    private String driverVersion;
    private String connectionUrl;
    private Map<String, Object> metadata;
    private String errorMessage;
  }

  @Data
  @Builder
  public static class KafkaInfo {
    private boolean available;
    private String version;
    private List<BrokerInfo> brokers;
    private SchemaRegistryInfo schemaRegistry;
    private List<String> topics;
    private Map<String, Object> configuration;
    private Map<String, Object> metadata; // Spring Kafka configuration
    private String errorMessage;
  }

  @Data
  @Builder
  public static class BrokerInfo {
    private int id;
    private String host;
    private int port;
    private boolean controller;
  }

  @Data
  @Builder
  public static class SchemaRegistryInfo {
    private boolean available;
    private String url;
    private String version;
    private int registeredSchemas;
    private Map<String, Object> metadata;
    private String errorMessage;
  }

  @Data
  @Builder
  public static class ComponentHealth {
    private String name;
    private boolean healthy;
    private String status;
    private String message;
  }

  @Data
  @Builder
  public static class PropertyInfo {
    private String key;
    private Object value;
    private String source;
    private String sourceType;
    private String resolvedValue;
  }
}
