package io.datahubproject.openapi.schema.registry;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.datahubproject.schema_registry.openapi.generated.CompatibilityCheckResponse;
import io.datahubproject.schema_registry.openapi.generated.ConfigUpdateRequest;
import io.datahubproject.schema_registry.openapi.generated.ModeUpdateRequest;
import io.datahubproject.schema_registry.openapi.generated.RegisterSchemaRequest;
import io.datahubproject.schema_registry.openapi.generated.RegisterSchemaResponse;
import io.swagger.api.RegistryApiController;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;


/**
 * DataHub implementation for Confluent's Schema Registry OpenAPI spec.
 */
@Slf4j
public class SchemaRegistryController extends RegistryApiController {
  public SchemaRegistryController(ObjectMapper objectMapper, HttpServletRequest request) {
    super(objectMapper, request);
  }

  @Override
  public ResponseEntity<Integer> deleteSchemaVersion(String subject, String version, Boolean permanent) {
    return super.deleteSchemaVersion(subject, version, permanent);
  }

  @Override
  public ResponseEntity<List<Integer>> deleteSubject(String subject, Boolean permanent) {
    return super.deleteSubject(subject, permanent);
  }

  @Override
  public ResponseEntity<String> deleteSubjectConfig(String subject) {
    return super.deleteSubjectConfig(subject);
  }

  @Override
  public ResponseEntity<String> deleteSubjectMode(String subject) {
    return super.deleteSubjectMode(subject);
  }

  @Override
  public ResponseEntity<Void> deleteTopLevelConfig() {
    return super.deleteTopLevelConfig();
  }

  @Override
  public ResponseEntity<String> get() {
    return super.get();
  }

  @Override
  public ResponseEntity<Void> getClusterId() {
    return super.getClusterId();
  }

  @Override
  public ResponseEntity<Void> getMode(String subject, Boolean defaultToGlobal) {
    return super.getMode(subject, defaultToGlobal);
  }

  @Override
  public ResponseEntity<Void> getReferencedBy(String subject, String version) {
    return super.getReferencedBy(subject, version);
  }

  @Override
  public ResponseEntity<Void> getSchema(Integer id, String subject, String format, Boolean fetchMaxId) {
    return super.getSchema(id, subject, format, fetchMaxId);
  }

  @Override
  public ResponseEntity<Void> getSchemaByVersion(String subject, String version, Boolean deleted) {
    return super.getSchemaByVersion(subject, version, deleted);
  }

  @Override
  public ResponseEntity<Void> getSchemaOnly(String subject, String version, Boolean deleted) {
    return super.getSchemaOnly(subject, version, deleted);
  }

  @Override
  public ResponseEntity<Void> getSchemaTypes() {
    return super.getSchemaTypes();
  }

  @Override
  public ResponseEntity<Void> getSchemas(String subjectPrefix, Boolean deleted, Boolean latestOnly, Integer offset,
      Integer limit) {
    return super.getSchemas(subjectPrefix, deleted, latestOnly, offset, limit);
  }

  @Override
  public ResponseEntity<Void> getSubjectLevelConfig(String subject, Boolean defaultToGlobal) {
    return super.getSubjectLevelConfig(subject, defaultToGlobal);
  }

  @Override
  public ResponseEntity<Void> getSubjects(Integer id, String subject, Boolean deleted) {
    return super.getSubjects(id, subject, deleted);
  }

  @Override
  public ResponseEntity<Void> getTopLevelConfig() {
    return super.getTopLevelConfig();
  }

  @Override
  public ResponseEntity<Void> getTopLevelMode() {
    return super.getTopLevelMode();
  }

  @Override
  public ResponseEntity<Void> getVersions(Integer id, String subject, Boolean deleted) {
    return super.getVersions(id, subject, deleted);
  }

  @Override
  public ResponseEntity<Void> list(String subjectPrefix, Boolean deleted) {
    return super.list(subjectPrefix, deleted);
  }

  @Override
  public ResponseEntity<Void> listContexts() {
    return super.listContexts();
  }

  @Override
  public ResponseEntity<Void> listVersions(String subject, Boolean deleted) {
    return super.listVersions(subject, deleted);
  }

  @Override
  public ResponseEntity<Schema> lookUpSchemaUnderSubject(String subject, RegisterSchemaRequest body, Boolean normalize,
      Boolean deleted) {
    return super.lookUpSchemaUnderSubject(subject, body, normalize, deleted);
  }

  @Override
  public ResponseEntity<Map<String, String>> post(Map<String, String> body) {
    return super.post(body);
  }

  @Override
  public ResponseEntity<RegisterSchemaResponse> register(String subject, RegisterSchemaRequest body,
      Boolean normalize) {
    return super.register(subject, body, normalize);
  }

  @Override
  public ResponseEntity<CompatibilityCheckResponse> testCompatibilityBySubjectName(String subject, String version,
      RegisterSchemaRequest body, Boolean verbose) {
    return super.testCompatibilityBySubjectName(subject, version, body, verbose);
  }

  @Override
  public ResponseEntity<CompatibilityCheckResponse> testCompatibilityForSubject(String subject,
      RegisterSchemaRequest body, Boolean verbose) {
    return super.testCompatibilityForSubject(subject, body, verbose);
  }

  @Override
  public ResponseEntity<Void> updateMode(String subject, ModeUpdateRequest body, Boolean force) {
    return super.updateMode(subject, body, force);
  }

  @Override
  public ResponseEntity<Void> updateSubjectLevelConfig(String subject, ConfigUpdateRequest body) {
    return super.updateSubjectLevelConfig(subject, body);
  }

  @Override
  public ResponseEntity<Void> updateTopLevelConfig(ConfigUpdateRequest body) {
    return super.updateTopLevelConfig(body);
  }

  @Override
  public ResponseEntity<Void> updateTopLevelMode(ModeUpdateRequest body, Boolean force) {
    return super.updateTopLevelMode(body, force);
  }
}
