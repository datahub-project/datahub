package io.datahubproject.openapi.schema.registry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.registry.SchemaRegistryService;
import io.datahubproject.schema_registry.openapi.generated.CompatibilityCheckResponse;
import io.datahubproject.schema_registry.openapi.generated.Config;
import io.datahubproject.schema_registry.openapi.generated.ConfigUpdateRequest;
import io.datahubproject.schema_registry.openapi.generated.Mode;
import io.datahubproject.schema_registry.openapi.generated.ModeUpdateRequest;
import io.datahubproject.schema_registry.openapi.generated.RegisterSchemaRequest;
import io.datahubproject.schema_registry.openapi.generated.RegisterSchemaResponse;
import io.datahubproject.schema_registry.openapi.generated.Schema;
import io.datahubproject.schema_registry.openapi.generated.SchemaString;
import io.datahubproject.schema_registry.openapi.generated.SubjectVersion;
import io.swagger.api.CompatibilityApi;
import io.swagger.api.ConfigApi;
import io.swagger.api.ContextsApi;
import io.swagger.api.DefaultApi;
import io.swagger.api.ModeApi;
import io.swagger.api.SchemasApi;
import io.swagger.api.SubjectsApi;
import io.swagger.api.V1Api;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** DataHub Rest Controller implementation for Confluent's Schema Registry OpenAPI spec. */
@Slf4j
@RestController
@RequestMapping("/schema-registry/api")
@Order(Ordered.HIGHEST_PRECEDENCE)
@RequiredArgsConstructor
public class SchemaRegistryController
    implements CompatibilityApi,
        ConfigApi,
        ContextsApi,
        DefaultApi,
        ModeApi,
        SchemasApi,
        SubjectsApi,
        V1Api {

  private final ObjectMapper objectMapper;

  private final HttpServletRequest request;

  @Qualifier("schemaRegistryService")
  private final SchemaRegistryService _schemaRegistryService;

  @PostConstruct
  public void init() {
    log.info("SchemaRegistryController initialized with base path: /schema-registry/api");
  }

  @Override
  public Optional<ObjectMapper> getObjectMapper() {
    return Optional.ofNullable(objectMapper);
  }

  @Override
  public Optional<HttpServletRequest> getRequest() {
    return Optional.ofNullable(request);
  }

  @Override
  public Optional<String> getAcceptHeader() {
    return CompatibilityApi.super.getAcceptHeader();
  }

  @Override
  public ResponseEntity<Void> getClusterId() {
    log.error("[V1API] getClusterId method not implemented");
    return V1Api.super.getClusterId();
  }

  @Override
  public ResponseEntity<Void> getSchemaRegistryVersion() {
    log.error("[V1API] getSchemaRegistryVersion method not implemented");
    return V1Api.super.getSchemaRegistryVersion();
  }

  @Override
  public ResponseEntity<Integer> deleteSchemaVersion(
      String subject, String version, Boolean permanent) {
    log.error("[SubjectsApi] deleteSchemaVersion method not implemented");
    return SubjectsApi.super.deleteSchemaVersion(subject, version, permanent);
  }

  @Override
  public ResponseEntity<List<Integer>> deleteSubject(String subject, Boolean permanent) {
    log.error("[SubjectsApi] deleteSubject method not implemented");
    return SubjectsApi.super.deleteSubject(subject, permanent);
  }

  @Override
  public ResponseEntity<List<Integer>> getReferencedBy(String subject, String version) {
    // DataHub doesn't currently track schema references, so return empty list
    // This could be enhanced in the future to track which schemas reference others
    return ResponseEntity.ok(List.of());
  }

  @Override
  public ResponseEntity<Schema> getSchemaByVersion(
      String subject, String version, Boolean deleted) {
    final String topicName = subject.replaceFirst("-value", "");

    // Handle "latest" version request
    if ("latest".equals(version)) {
      Optional<Integer> latestVersionOpt =
          _schemaRegistryService.getLatestSchemaVersionForTopic(topicName);
      if (!latestVersionOpt.isPresent()) {
        log.error(
            "[SubjectsApi] getSchemaByVersion couldn't find latest version for topic {}.",
            topicName);
        return new ResponseEntity<>(HttpStatus.NOT_FOUND);
      }

      int latestVersion = latestVersionOpt.get();
      return _schemaRegistryService
          .getSchemaForTopicAndVersion(topicName, latestVersion)
          .map(
              schema -> {
                Schema result = new Schema();
                result.setSubject(subject);
                result.setVersion(latestVersion);
                result.setId(_schemaRegistryService.getSchemaIdForTopic(topicName).get());
                result.setSchema(schema.toString());
                return new ResponseEntity<>(result, HttpStatus.OK);
              })
          .orElseGet(
              () -> {
                log.error("[SubjectsApi] getSchemaByVersion couldn't find topic {}.", topicName);
                return new ResponseEntity<>(HttpStatus.NOT_FOUND);
              });
    }

    // Handle specific version request
    try {
      int versionNumber = Integer.parseInt(version);

      // Check if this version is supported for the topic
      Optional<List<Integer>> supportedVersions =
          _schemaRegistryService.getSupportedSchemaVersionsForTopic(topicName);
      if (supportedVersions.isPresent() && !supportedVersions.get().contains(versionNumber)) {
        log.error(
            "[SubjectsApi] getSchemaByVersion subject {} version {} not supported for topic {}.",
            subject,
            version,
            topicName);
        return new ResponseEntity<>(HttpStatus.NOT_FOUND);
      }

      return _schemaRegistryService
          .getSchemaForTopicAndVersion(topicName, versionNumber)
          .map(
              schema -> {
                Schema result = new Schema();
                result.setSubject(subject);
                result.setVersion(versionNumber);
                result.setId(_schemaRegistryService.getSchemaIdForTopic(topicName).get());
                result.setSchema(schema.toString());
                return new ResponseEntity<>(result, HttpStatus.OK);
              })
          .orElseGet(
              () -> {
                log.error(
                    "[SubjectsApi] getSchemaByVersion couldn't find topic {} version {}.",
                    topicName,
                    version);
                return new ResponseEntity<>(HttpStatus.NOT_FOUND);
              });
    } catch (NumberFormatException e) {
      log.error("[SubjectsApi] getSchemaByVersion invalid version format: {}", version);
      return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
    }
  }

  /**
   * Get schema by ID - CRITICAL for Kafka consumer deserialization This is the standard endpoint
   * consumers use to resolve schemas by ID When a message has schema ID 1, consumers call this to
   * get the exact schema
   */
  @Override
  public ResponseEntity<SchemaString> getSchema(
      @Parameter(
              in = ParameterIn.PATH,
              description = "Globally unique identifier of the schema",
              required = true,
              schema = @io.swagger.v3.oas.annotations.media.Schema())
          @PathVariable("id")
          Integer id,
      @Parameter(
              in = ParameterIn.QUERY,
              description = "Name of the subject",
              schema = @io.swagger.v3.oas.annotations.media.Schema())
          @Valid
          @RequestParam(value = "subject", required = false)
          String subject,
      @Parameter(
              in = ParameterIn.QUERY,
              description = "Desired output format, dependent on schema type",
              schema = @io.swagger.v3.oas.annotations.media.Schema())
          @Valid
          @RequestParam(value = "format", required = false)
          String format,
      @Parameter(
              in = ParameterIn.QUERY,
              description = "Whether to fetch the maximum schema identifier that exists",
              schema = @io.swagger.v3.oas.annotations.media.Schema(defaultValue = "false"))
          @Valid
          @RequestParam(value = "fetchMaxId", required = false, defaultValue = "false")
          Boolean fetchMaxId) {

    // Get the topic name for this schema ID
    Optional<String> topicNameOpt = _schemaRegistryService.getTopicNameById(id);
    if (!topicNameOpt.isPresent()) {
      log.error("Schema not found for ID {}", id);
      return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    }

    String topicName = topicNameOpt.get();

    // Get the schema for this topic (this will be the schema associated with the ID)
    Optional<org.apache.avro.Schema> schemaOpt = _schemaRegistryService.getSchemaForId(id);
    if (!schemaOpt.isPresent()) {
      log.error("Schema not found for ID {} and topic {}", id, topicName);
      return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    }

    // Get the latest version for this topic to determine the subject
    Optional<Integer> latestVersionOpt =
        _schemaRegistryService.getLatestSchemaVersionForTopic(topicName);
    if (!latestVersionOpt.isPresent()) {
      log.error("No version found for topic {}", topicName);
      return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    }

    // Create SchemaString response (this is what the generated interface expects)
    SchemaString result = new SchemaString();
    result.setSchema(schemaOpt.get().toString());
    result.setSchemaType("AVRO");

    // Set maxId if fetchMaxId is true
    if (Boolean.TRUE.equals(fetchMaxId)) {
      result.setMaxId(id); // For now, just use the current ID as max
    }

    return ResponseEntity.ok(result);
  }

  @Override
  public ResponseEntity<String> getSchemaOnly2(String subject, String version, Boolean deleted) {
    try {
      int versionNumber = Integer.parseInt(version);

      // Try to get schema by subject and version first (most direct approach)
      Optional<org.apache.avro.Schema> schemaOpt =
          _schemaRegistryService.getSchemaBySubjectAndVersion(subject, versionNumber);

      if (schemaOpt.isPresent()) {
        return ResponseEntity.ok(schemaOpt.get().toString());
      }

      // Fallback to topic-based lookup
      final String topicName = subject.replaceFirst("-value", "");
      schemaOpt = _schemaRegistryService.getSchemaForTopicAndVersion(topicName, versionNumber);

      if (schemaOpt.isPresent()) {
        return ResponseEntity.ok(schemaOpt.get().toString());
      }

      log.error("Schema not found for subject {} version {}", subject, version);
      return new ResponseEntity<>(HttpStatus.NOT_FOUND);

    } catch (NumberFormatException e) {
      log.error("Invalid version format: {}", version);
      return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
    }
  }

  @Override
  public ResponseEntity<List<String>> list(
      @Parameter(
              in = ParameterIn.QUERY,
              description = "Subject name prefix",
              schema = @io.swagger.v3.oas.annotations.media.Schema(defaultValue = ":*:"))
          @Valid
          @RequestParam(value = "subjectPrefix", required = false, defaultValue = ":*:")
          String subjectPrefix,
      @Parameter(
              in = ParameterIn.QUERY,
              description = "Whether to look up deleted subjects",
              schema = @io.swagger.v3.oas.annotations.media.Schema())
          @Valid
          @RequestParam(value = "deleted", required = false)
          Boolean deleted,
      @Parameter(
              in = ParameterIn.QUERY,
              description = "Whether to return deleted subjects only",
              schema = @io.swagger.v3.oas.annotations.media.Schema())
          @Valid
          @RequestParam(value = "deletedOnly", required = false)
          Boolean deletedOnly) {
    // If deletedOnly is true, return empty list since we don't support deleted schemas
    if (Boolean.TRUE.equals(deletedOnly)) {
      return ResponseEntity.ok(List.of());
    }

    // If deleted is true, return empty list since we don't support deleted schemas
    if (Boolean.TRUE.equals(deleted)) {
      return ResponseEntity.ok(List.of());
    }

    // Get all topics and convert them to subjects (add "-value" suffix)
    List<String> topics = _schemaRegistryService.getAllTopics();

    if (topics == null) {
      return ResponseEntity.ok(List.of());
    }

    List<String> subjects =
        topics.stream()
            .map(topic -> topic + "-value")
            .filter(
                subject -> {
                  // Handle the special ":*:" default value and null cases
                  if (subjectPrefix == null || ":*:".equals(subjectPrefix)) {
                    return true; // Include all subjects
                  }
                  return subject.startsWith(subjectPrefix);
                })
            .collect(Collectors.toList());

    return ResponseEntity.ok(subjects);
  }

  @Override
  public ResponseEntity<List<Integer>> listVersions(
      String subject, Boolean deleted, Boolean deletedOnly) {
    final String topicName = subject.replaceFirst("-value", "");
    return _schemaRegistryService
        .getSupportedSchemaVersionsForTopic(topicName)
        .map(
            versions -> {
              return new ResponseEntity<>(versions, HttpStatus.OK);
            })
        .orElseGet(
            () -> {
              log.error("[SubjectsApi] listVersions couldn't find topic with name {}.", topicName);
              return new ResponseEntity<>(HttpStatus.NOT_FOUND);
            });
  }

  @Override
  public ResponseEntity<Schema> lookUpSchemaUnderSubject(
      String subject, RegisterSchemaRequest body, Boolean normalize, Boolean deleted) {
    log.error("[SubjectsApi] lookUpSchemaUnderSubject method not implemented");
    return SubjectsApi.super.lookUpSchemaUnderSubject(subject, body, normalize, deleted);
  }

  @Override
  public ResponseEntity<Mode> deleteSubjectMode(String subject) {
    log.error("[ModeApi] deleteSubjectMode method not implemented");
    return ModeApi.super.deleteSubjectMode(subject);
  }

  @Override
  public ResponseEntity<Mode> getMode(String subject, Boolean defaultToGlobal) {
    log.error("[ModeApi] getMode method not implemented");
    return ModeApi.super.getMode(subject, defaultToGlobal);
  }

  @Override
  public ResponseEntity<Mode> getTopLevelMode() {
    log.error("[ModeApi] getTopLevelMode method not implemented");
    return ModeApi.super.getTopLevelMode();
  }

  @Override
  public ResponseEntity<ModeUpdateRequest> updateMode(
      String subject, ModeUpdateRequest body, Boolean force) {
    log.error("[ModeApi] updateMode method not implemented");
    return ModeApi.super.updateMode(subject, body, force);
  }

  @Override
  public ResponseEntity<ModeUpdateRequest> updateTopLevelMode(
      ModeUpdateRequest body, Boolean force) {
    log.error("[ModeApi] updateTopLevelMode method not implemented");
    return ModeApi.super.updateTopLevelMode(body, force);
  }

  @Override
  @Operation(
      summary = "Schema Registry Root Resource",
      description = "The Root resource is a no-op, only used to " + "validate endpoint is ready.",
      tags = {"Schema Registry Base"})
  public ResponseEntity<String> get() {
    return new ResponseEntity<>(HttpStatus.OK);
  }

  @Override
  @Operation(
      summary = "",
      description = "",
      tags = {"Schema Registry Base"})
  public ResponseEntity<Map<String, String>> post(Map<String, String> body) {
    log.error("[DefaultApi] post method not implemented");
    return DefaultApi.super.post(body);
  }

  @Override
  public ResponseEntity<List<String>> listContexts() {
    log.error("[ContextsApi] listContexts method not implemented");
    return ContextsApi.super.listContexts();
  }

  @Override
  public ResponseEntity<String> deleteSubjectConfig(String subject) {
    log.error("[ConfigApi] deleteSubjectConfig method not implemented");
    return ConfigApi.super.deleteSubjectConfig(subject);
  }

  @Override
  public ResponseEntity<String> deleteTopLevelConfig() {
    log.error("[ConfigApi] deleteTopLevelConfig method not implemented");
    return ConfigApi.super.deleteTopLevelConfig();
  }

  @Override
  public ResponseEntity<Config> getSubjectLevelConfig(String subject, Boolean defaultToGlobal) {
    final String topicName = subject.replaceFirst("-value", "");

    // Get the compatibility level for the topic
    String compatibilityLevel = _schemaRegistryService.getSchemaCompatibility(topicName);

    // If defaultToGlobal is true and no compatibility level is set, return global default
    if (Boolean.TRUE.equals(defaultToGlobal) && compatibilityLevel == null) {
      return getTopLevelConfig();
    }

    Config config = new Config();
    config.setCompatibilityLevel(Config.CompatibilityLevelEnum.fromValue(compatibilityLevel));

    return ResponseEntity.ok(config);
  }

  @Override
  public ResponseEntity<Config> getTopLevelConfig() {
    return ResponseEntity.ok(
        new Config().compatibilityLevel(Config.CompatibilityLevelEnum.BACKWARD));
  }

  @Override
  public ResponseEntity<ConfigUpdateRequest> updateSubjectLevelConfig(
      String subject, ConfigUpdateRequest body) {
    log.error("[ConfigApi] updateSubjectLevelConfig method not implemented");
    return ConfigApi.super.updateSubjectLevelConfig(subject, body);
  }

  @Override
  public ResponseEntity<ConfigUpdateRequest> updateTopLevelConfig(ConfigUpdateRequest body) {
    log.error("[ConfigApi] updateTopLevelConfig method not implemented");
    return ConfigApi.super.updateTopLevelConfig(body);
  }

  @Override
  public ResponseEntity<CompatibilityCheckResponse> testCompatibilityBySubjectName(
      String subject, String version, RegisterSchemaRequest body, Boolean verbose) {
    final String topicName = subject.replaceFirst("-value", "");

    // Parse the version
    int versionNumber;
    try {
      versionNumber = Integer.parseInt(version);
    } catch (NumberFormatException e) {
      log.error(
          "[CompatibilityApi] testCompatibilityBySubjectName invalid version format: {}", version);
      return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
    }

    // Get the specific schema version for the topic
    Optional<org.apache.avro.Schema> currentSchema =
        _schemaRegistryService.getSchemaForTopicAndVersion(topicName, versionNumber);
    if (!currentSchema.isPresent()) {
      log.error(
          "[CompatibilityApi] testCompatibilityBySubjectName couldn't find topic {} version {}.",
          topicName,
          version);
      return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    }

    // Parse the new schema from the request body
    if (body == null || body.getSchema() == null) {
      log.error("[CompatibilityApi] testCompatibilityBySubjectName missing schema in request body");
      return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
    }

    try {
      org.apache.avro.Schema newSchema =
          new org.apache.avro.Schema.Parser().parse(body.getSchema());

      // Perform actual compatibility check
      boolean isCompatible =
          _schemaRegistryService.checkSchemaCompatibility(
              topicName, newSchema, currentSchema.get());

      CompatibilityCheckResponse response = new CompatibilityCheckResponse();
      response.setIsCompatible(isCompatible);

      if (Boolean.TRUE.equals(verbose)) {
        if (isCompatible) {
          response.setMessages(List.of("Schema is compatible with version " + version));
        } else {
          response.setMessages(
              List.of(
                  "Schema is NOT compatible with version "
                      + version
                      + ". Check schema evolution rules."));
        }
      }

      return ResponseEntity.ok(response);

    } catch (Exception e) {
      log.error(
          "[CompatibilityApi] testCompatibilityBySubjectName failed to parse schema: {}",
          e.getMessage(),
          e);
      return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
    }
  }

  @Override
  public ResponseEntity<CompatibilityCheckResponse> testCompatibilityForSubject(
      String subject, RegisterSchemaRequest body, Boolean verbose) {
    final String topicName = subject.replaceFirst("-value", "");

    // Get the current schema for the topic
    Optional<org.apache.avro.Schema> currentSchema =
        _schemaRegistryService.getSchemaForTopic(topicName);
    if (!currentSchema.isPresent()) {
      log.error(
          "[CompatibilityApi] testCompatibilityForSubject couldn't find topic {}.", topicName);
      return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    }

    // Parse the new schema from the request body
    if (body == null || body.getSchema() == null) {
      log.error("[CompatibilityApi] testCompatibilityForSubject missing schema in request body");
      return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
    }

    try {
      org.apache.avro.Schema newSchema =
          new org.apache.avro.Schema.Parser().parse(body.getSchema());

      // Perform actual compatibility check
      boolean isCompatible =
          _schemaRegistryService.checkSchemaCompatibility(
              topicName, newSchema, currentSchema.get());

      CompatibilityCheckResponse response = new CompatibilityCheckResponse();
      response.setIsCompatible(isCompatible);

      if (Boolean.TRUE.equals(verbose)) {
        if (isCompatible) {
          response.setMessages(List.of("Schema is compatible with current schema"));
        } else {
          response.setMessages(
              List.of(
                  "Schema is NOT compatible with current schema. "
                      + "Check schema evolution rules."));
        }
      }

      return ResponseEntity.ok(response);

    } catch (Exception e) {
      log.error(
          "[CompatibilityApi] testCompatibilityForSubject failed to parse schema: {}",
          e.getMessage(),
          e);
      return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
    }
  }

  @Override
  public ResponseEntity<RegisterSchemaResponse> register(
      String subject, RegisterSchemaRequest body, Boolean normalize) {
    final String topicName = subject.replaceFirst("-value", "");
    return _schemaRegistryService
        .getSchemaIdForTopic(topicName)
        .map(
            id -> {
              final RegisterSchemaResponse response = new RegisterSchemaResponse();
              return new ResponseEntity<>(response.id(id), HttpStatus.OK);
            })
        .orElseGet(
            () -> {
              if (topicName.matches("^[a-zA-Z0-9._-]+$")) {
                log.error("Couldn't find topic with name {}.", topicName);
              } else {
                log.error("Couldn't find topic (Malformed topic name)");
              }
              return new ResponseEntity<>(HttpStatus.NOT_FOUND);
            });
  }

  @Override
  public ResponseEntity<String> getSchemaOnly(Integer id, String subject, String format) {
    return _schemaRegistryService
        .getSchemaForId(id)
        .map(
            schema -> {
              // Return just the schema string
              return new ResponseEntity<>(schema.toString(), HttpStatus.OK);
            })
        .orElseGet(
            () -> {
              log.error("[SchemasApi] getSchemaOnly couldn't find schema with id {}.", id);
              return new ResponseEntity<>(HttpStatus.NOT_FOUND);
            });
  }

  @Override
  public ResponseEntity<List<String>> getSchemaTypes() {
    // DataHub currently only supports AVRO schemas
    return ResponseEntity.ok(List.of("AVRO"));
  }

  @Override
  public ResponseEntity<List<Schema>> getSchemas(
      String subjectPrefix, Boolean deleted, Boolean latestOnly, Integer offset, Integer limit) {
    // If deleted is true, return empty list since we don't support deleted schemas
    if (Boolean.TRUE.equals(deleted)) {
      return ResponseEntity.ok(List.of());
    }

    // Get all topics and convert them to subjects
    List<String> topics = _schemaRegistryService.getAllTopics();
    List<String> subjects =
        topics.stream()
            .map(topic -> topic + "-value")
            .filter(
                subject -> {
                  if (subjectPrefix == null || ":*:".equals(subjectPrefix)) {
                    return true; // Include all subjects
                  }
                  return subject.startsWith(subjectPrefix);
                })
            .collect(Collectors.toList());

    // Convert subjects to Schema objects
    List<Schema> schemas =
        subjects.stream()
            .map(
                subject -> {
                  String topicName = subject.replaceFirst("-value", "");
                  Optional<Integer> schemaId =
                      _schemaRegistryService.getSchemaIdForTopic(topicName);
                  Optional<org.apache.avro.Schema> schema =
                      _schemaRegistryService.getSchemaForTopic(topicName);

                  if (schemaId.isPresent() && schema.isPresent()) {
                    Schema result = new Schema();
                    result.setSubject(subject);
                    result.setId(schemaId.get());
                    result.setSchema(schema.get().toString());

                    // Set version based on latestOnly flag
                    if (Boolean.TRUE.equals(latestOnly)) {
                      Optional<Integer> latestVersion =
                          _schemaRegistryService.getLatestSchemaVersionForTopic(topicName);
                      latestVersion.ifPresent(result::setVersion);
                    } else {
                      // For now, set version to 1 if not latestOnly
                      result.setVersion(1);
                    }

                    return result;
                  }
                  return null;
                })
            .filter(schema -> schema != null)
            .collect(Collectors.toList());

    // Apply offset and limit if provided
    if (offset != null && offset > 0 && offset < schemas.size()) {
      schemas = schemas.subList(offset, schemas.size());
    }
    if (limit != null && limit > 0 && limit < schemas.size()) {
      schemas = schemas.subList(0, limit);
    }

    return ResponseEntity.ok(schemas);
  }

  @Override
  public ResponseEntity<List<String>> getSubjects(Integer id, String subject, Boolean deleted) {
    // If deleted is true, return empty list since we don't support deleted schemas
    if (Boolean.TRUE.equals(deleted)) {
      return ResponseEntity.ok(List.of());
    }

    if (id != null) {
      // Get subject for a specific schema ID
      Optional<String> topicNameOpt = _schemaRegistryService.getTopicNameById(id);
      if (topicNameOpt.isPresent()) {
        String subjectName = topicNameOpt.get() + "-value";
        return ResponseEntity.ok(List.of(subjectName));
      } else {
        log.error("[SchemasApi] getSubjects couldn't find schema with id {}.", id);
        return new ResponseEntity<>(HttpStatus.NOT_FOUND);
      }
    } else if (subject != null) {
      // If subject is provided, return it if it exists
      final String topicName = subject.replaceFirst("-value", "");
      Optional<Integer> schemaId = _schemaRegistryService.getSchemaIdForTopic(topicName);
      if (schemaId.isPresent()) {
        return ResponseEntity.ok(List.of(subject));
      } else {
        log.error("[SchemasApi] getSubjects couldn't find subject {}.", subject);
        return new ResponseEntity<>(HttpStatus.NOT_FOUND);
      }
    }

    // If neither id nor subject is provided, return all subjects
    List<String> topics = _schemaRegistryService.getAllTopics();
    List<String> subjects =
        topics.stream().map(topic -> topic + "-value").collect(Collectors.toList());
    return ResponseEntity.ok(subjects);
  }

  @Override
  public ResponseEntity<List<SubjectVersion>> getVersions(
      Integer id, String subject, Boolean deleted) {
    // If deleted is true, return empty list since we don't support deleted schemas
    if (Boolean.TRUE.equals(deleted)) {
      return ResponseEntity.ok(List.of());
    }

    if (subject != null) {
      // Get versions for a specific subject
      final String topicName = subject.replaceFirst("-value", "");
      Optional<List<Integer>> supportedVersions =
          _schemaRegistryService.getSupportedSchemaVersionsForTopic(topicName);

      if (supportedVersions.isPresent()) {
        List<SubjectVersion> subjectVersions =
            supportedVersions.get().stream()
                .map(
                    version -> {
                      SubjectVersion sv = new SubjectVersion();
                      sv.setSubject(subject);
                      sv.setVersion(version);
                      return sv;
                    })
                .collect(Collectors.toList());
        return ResponseEntity.ok(subjectVersions);
      } else {
        log.error("[SchemasApi] getVersions couldn't find topic {}.", topicName);
        return new ResponseEntity<>(HttpStatus.NOT_FOUND);
      }
    } else if (id != null) {
      // Get versions for a specific schema ID
      Optional<String> topicNameOpt = _schemaRegistryService.getTopicNameById(id);
      if (topicNameOpt.isPresent()) {
        Optional<List<Integer>> supportedVersions =
            _schemaRegistryService.getSupportedSchemaVersionsForTopic(topicNameOpt.get());
        if (supportedVersions.isPresent()) {
          List<SubjectVersion> subjectVersions =
              supportedVersions.get().stream()
                  .map(
                      version -> {
                        SubjectVersion sv = new SubjectVersion();
                        sv.setSubject(topicNameOpt.get() + "-value");
                        sv.setVersion(version);
                        return sv;
                      })
                  .collect(Collectors.toList());
          return ResponseEntity.ok(subjectVersions);
        }
      }
      log.error("[SchemasApi] getVersions couldn't find schema with id {}.", id);
      return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    }

    // If neither subject nor id is provided, return empty list
    return ResponseEntity.ok(List.of());
  }
}
