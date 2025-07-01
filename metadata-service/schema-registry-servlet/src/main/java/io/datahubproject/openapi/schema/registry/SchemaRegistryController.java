package io.datahubproject.openapi.schema.registry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
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
import jakarta.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
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
  private static final Set<String> SCHEMA_VERSIONS =
      ImmutableSet.of(String.valueOf(Constants.FIXED_SCHEMA_VERSION), "latest");

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
    log.error("[SubjectsApi] getReferencedBy method not implemented");
    return SubjectsApi.super.getReferencedBy(subject, version);
  }

  @Override
  public ResponseEntity<Schema> getSchemaByVersion(
      String subject, String version, Boolean deleted) {
    final String topicName = subject.replaceFirst("-value", "");

    if (!SCHEMA_VERSIONS.contains(version)) {
      log.error(
          "[SubjectsApi] getSchemaByVersion subject {} version {} not found.", subject, version);
      return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    }

    return _schemaRegistryService
        .getSchemaForTopic(topicName)
        .map(
            schema -> {
              Schema result = new Schema();
              result.setSubject(subject);
              result.setVersion(Constants.FIXED_SCHEMA_VERSION);
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

  @Override
  public ResponseEntity<String> getSchemaOnly2(String subject, String version, Boolean deleted) {
    log.error("[SubjectsApi] getSchemaOnly2 method not implemented");
    return SubjectsApi.super.getSchemaOnly2(subject, version, deleted);
  }

  @Override
  public ResponseEntity<List<String>> list(
      String subjectPrefix, Boolean deleted, Boolean deletedOnly) {
    log.error("[SubjectsApi] list method not implemented");
    return SubjectsApi.super.list(subjectPrefix, deleted, deletedOnly);
  }

  @Override
  public ResponseEntity<List<Integer>> listVersions(
      String subject, Boolean deleted, Boolean deletedOnly) {
    final String topicName = subject.replaceFirst("-value", "");
    return _schemaRegistryService
        .getSchemaForTopic(topicName)
        .map(
            schema -> {
              return new ResponseEntity<>(
                  Arrays.asList(Constants.FIXED_SCHEMA_VERSION), HttpStatus.OK);
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
    return getTopLevelConfig();
  }

  @RequestMapping(
      value = {"/config", "/config/"},
      produces = {
        "application/vnd.schemaregistry.v1+json",
        "application/vnd.schemaregistry+json; qs=0.9",
        "application/json; qs=0.5"
      },
      method = RequestMethod.GET)
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
    log.error("[CompatibilityApi] testCompatibilityBySubjectName method not implemented");
    return CompatibilityApi.super.testCompatibilityBySubjectName(subject, version, body, verbose);
  }

  @Override
  public ResponseEntity<CompatibilityCheckResponse> testCompatibilityForSubject(
      String subject, RegisterSchemaRequest body, Boolean verbose) {
    log.error("[CompatibilityApi] testCompatibilityForSubject method not implemented");
    return CompatibilityApi.super.testCompatibilityForSubject(subject, body, verbose);
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
  public ResponseEntity<SchemaString> getSchema(
      Integer id, String subject, String format, Boolean fetchMaxId) {
    return _schemaRegistryService
        .getSchemaForId(id)
        .map(
            schema -> {
              SchemaString result = new SchemaString();
              if (fetchMaxId) {
                result.setMaxId(id);
              }
              result.setSchema(schema.toString());
              return new ResponseEntity<>(result, HttpStatus.OK);
            })
        .orElseGet(
            () -> {
              log.error("Couldn't find topic with id {}.", id);
              return new ResponseEntity<>(HttpStatus.NOT_FOUND);
            });
  }

  @Override
  public ResponseEntity<String> getSchemaOnly(Integer id, String subject, String format) {
    log.error("[SchemasApi] getSchemaOnly method not implemented");
    return SchemasApi.super.getSchemaOnly(id, subject, format);
  }

  @Override
  public ResponseEntity<List<String>> getSchemaTypes() {
    log.error("[SchemasApi] getSchemaTypes method not implemented");
    return SchemasApi.super.getSchemaTypes();
  }

  @Override
  public ResponseEntity<List<Schema>> getSchemas(
      String subjectPrefix, Boolean deleted, Boolean latestOnly, Integer offset, Integer limit) {
    log.error("[SchemasApi] getSchemas method not implemented");
    return SchemasApi.super.getSchemas(subjectPrefix, deleted, latestOnly, offset, limit);
  }

  @Override
  public ResponseEntity<List<String>> getSubjects(Integer id, String subject, Boolean deleted) {
    log.error("[SchemasApi] getSubjects method not implemented");
    return SchemasApi.super.getSubjects(id, subject, deleted);
  }

  @Override
  public ResponseEntity<List<SubjectVersion>> getVersions(
      Integer id, String subject, Boolean deleted) {
    log.error("[SchemasApi] getVersions method not implemented");
    return SchemasApi.super.getVersions(id, subject, deleted);
  }
}
