package com.linkedin.metadata.entity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.retention.IndefiniteRetention;
import com.linkedin.metadata.retention.Retention;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class LocalRetentionService implements RetentionService {
  private final List<Retention> defaultRetention;
  private final Map<String, List<Retention>> retentionPerEntity;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());
  private static final List<Retention> INDEFINITE_RETENTION =
      ImmutableList.of(new Retention().setIndefinite(new IndefiniteRetention()));

  public LocalRetentionService(EntityRegistry entityRegistry) {
    this(entityRegistry, LocalRetentionService.class.getResourceAsStream("/retention.yaml"));
  }

  public LocalRetentionService(EntityRegistry entityRegistry, String configPath) throws FileNotFoundException {
    this(entityRegistry, new FileInputStream(configPath));
  }

  public LocalRetentionService(EntityRegistry entityRegistry, InputStream configFileStream) {
    ObjectNode retentionConfig;
    try {
      retentionConfig = (ObjectNode) OBJECT_MAPPER.readTree(configFileStream);
    } catch (IOException e) {
      log.error("Issue while reading retention config file", e);
      throw new IllegalArgumentException(e);
    }

    if (retentionConfig.has("default")) {
      JsonNode defaultRetentionInput = retentionConfig.get("default");
      if (!defaultRetentionInput.isArray()) {
        throw new IllegalArgumentException("Default setup must contain a list of retention policies");
      }
      defaultRetention = buildRetentionList((ArrayNode) defaultRetentionInput);
      validateRetentionList(defaultRetention);
    } else {
      defaultRetention = INDEFINITE_RETENTION;
    }

    if (retentionConfig.has("entity")) {
      retentionPerEntity = new HashMap<>();
      JsonNode retentionPerEntityInput = retentionConfig.get("entity");
      if (!retentionPerEntityInput.isArray()) {
        throw new IllegalArgumentException("entity field must be a list");
      }
      Iterator<JsonNode> it = retentionPerEntityInput.elements();
      while (it.hasNext()) {
        JsonNode retentionForEntity = it.next();
        if (!retentionForEntity.has("name") || !retentionForEntity.has("retention")) {
          throw new IllegalArgumentException("Each element of the entity list must specify entity name in field name");
        }
        String entityName = retentionForEntity.get("name").asText();
        if (!entityRegistry.getEntitySpecs().containsKey(entityName.toLowerCase())) {
          throw new IllegalArgumentException("Invalid entity name: " + entityName);
        }
        if (!retentionForEntity.has("retention") || !retentionForEntity.get("retention").isArray()) {
          throw new IllegalArgumentException(
              "Each element of the entity list must specify retention policies in field retention");
        }
        List<Retention> retentionList = buildRetentionList((ArrayNode) retentionForEntity.get("retention"));
        validateRetentionList(retentionList);
        retentionPerEntity.put(entityName.toLowerCase(), retentionList);
      }
    } else {
      retentionPerEntity = Collections.emptyMap();
    }
  }

  private List<Retention> buildRetentionList(ArrayNode retentionArray) {
    List<Retention> retentionList = new ArrayList<>();
    Iterator<JsonNode> it = retentionArray.elements();
    while (it.hasNext()) {
      retentionList.add(buildRetention(it.next()));
    }
    if (retentionList.isEmpty()) {
      return INDEFINITE_RETENTION;
    }
    return retentionList;
  }

  private Retention buildRetention(JsonNode retention) {
    return RecordUtils.toRecordTemplate(Retention.class, retention.toString());
  }

  private void validateRetentionList(List<Retention> retentionList) {
    Set<String> retentionsSoFar = new HashSet<>();
    for (Retention retention : retentionList) {
      if (retention.data().size() != 1) {
        throw new IllegalArgumentException("Exactly one retention policy should be set per element");
      }
      if (retention.hasIndefinite() && retentionList.size() > 1) {
        throw new IllegalArgumentException("Indefinite policy cannot be combined with any other policy");
      }
      String retentionType = retention.data().keySet().stream().findFirst().get();
      if (retentionsSoFar.contains(retentionType)) {
        throw new IllegalArgumentException("Type of policies in the list must be unique");
      }
      retentionsSoFar.add(retentionType);
      validateRetention(retention);
    }
  }

  private void validateRetention(Retention retention) {
    if (retention.hasVersion()) {
      if (retention.getVersion().getMaxVersions() <= 0) {
        throw new IllegalArgumentException("Invalid maxVersions: " + retention.getVersion().getMaxVersions());
      }
    }
    if (retention.hasTime()) {
      if (retention.getTime().getMaxAgeInSeconds() <= 0) {
        throw new IllegalArgumentException("Invalid maxAgeInSeconds: " + retention.getTime().getMaxAgeInSeconds());
      }
    }
  }

  @Override
  public List<Retention> getRetention(String entityName, String aspectName) {
    return retentionPerEntity.getOrDefault(entityName.toLowerCase(), defaultRetention);
  }
}
