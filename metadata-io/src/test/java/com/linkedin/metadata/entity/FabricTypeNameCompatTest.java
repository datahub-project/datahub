package com.linkedin.metadata.entity;

import static io.datahubproject.test.search.SearchTestUtils.V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.OpenSearchSearchClientShim;
import com.linkedin.metadata.search.elasticsearch.index.DelegatingMappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MultiEntityMappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.RecordTemplateValidator;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.opensearch.common.settings.Settings;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Proves that moving env / origin fields from the {@code FabricType} enum to the {@code
 * FabricTypeName} string typeref needs no data migration. Stored aspects for built-in envs
 * round-trip, and each env / origin search field keeps a keyword mapping an enum field produced.
 *
 * <p>{@value #ENV_FIELD_MAPPINGS} is a one-time fixture of those field mappings only. A synthetic
 * index holding just that field is what {@link ReindexConfig} compares. Other searchable fields on
 * the same index are not part of the fixture.
 */
public class FabricTypeNameCompatTest {

  private static final ObjectMapper MAPPER =
      new ObjectMapper().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
  private static final String ENV_FIELD_MAPPINGS = "/fabric-type-compat/env-field-mappings.json";
  private static final String V3_REPRESENTATIVE_INDEX = "datasetindex_v3";

  /** Env / origin fields this change moved to FabricTypeName. Other env fields may also exist. */
  private static final List<String> REQUIRED_ENV_FIELDS =
      List.of(
          "containerindex_v2 properties.env",
          "dataflowindex_v2 properties.env",
          "datajobindex_v2 properties.env",
          "dataprocessindex_v2 properties.origin",
          "datasetindex_v2 properties.origin",
          "mlmodeldeploymentindex_v2 properties.origin",
          "mlmodelgroupindex_v2 properties.origin",
          "mlmodelindex_v2 properties.origin",
          "datasetindex_v3 properties.env",
          "datasetindex_v3 properties.origin",
          "datasetindex_v3 properties._aspects.properties.containerProperties.properties.env",
          "datasetindex_v3 properties._aspects.properties.dataFlowInfo.properties.env",
          "datasetindex_v3 properties._aspects.properties.dataJobInfo.properties.env",
          "datasetindex_v3 properties._aspects.properties.dataProcessKey.properties.origin",
          "datasetindex_v3 properties._aspects.properties.datasetKey.properties.origin",
          "datasetindex_v3 properties._aspects.properties.mlModelDeploymentKey.properties.origin",
          "datasetindex_v3 properties._aspects.properties.mlModelGroupKey.properties.origin",
          "datasetindex_v3 properties._aspects.properties.mlModelKey.properties.origin");

  /** Every aspect whose env / origin field moved to FabricTypeName, with a minimal valid body. */
  private static final List<Case> CASES =
      List.of(
          new Case(
              "dataset",
              "datasetKey",
              "urn:li:dataset:(urn:li:dataPlatform:hive,db.tbl,%s)",
              "{\"platform\":\"urn:li:dataPlatform:hive\",\"name\":\"db.tbl\",\"origin\":\"%s\"}"),
          new Case(
              "mlModel",
              "mlModelKey",
              "urn:li:mlModel:(urn:li:dataPlatform:sagemaker,model,%s)",
              "{\"platform\":\"urn:li:dataPlatform:sagemaker\",\"name\":\"model\",\"origin\":\"%s\"}"),
          new Case(
              "mlModelGroup",
              "mlModelGroupKey",
              "urn:li:mlModelGroup:(urn:li:dataPlatform:sagemaker,group,%s)",
              "{\"platform\":\"urn:li:dataPlatform:sagemaker\",\"name\":\"group\",\"origin\":\"%s\"}"),
          new Case(
              "mlModelDeployment",
              "mlModelDeploymentKey",
              "urn:li:mlModelDeployment:(urn:li:dataPlatform:sagemaker,deployment,%s)",
              "{\"platform\":\"urn:li:dataPlatform:sagemaker\",\"name\":\"deployment\","
                  + "\"origin\":\"%s\"}"),
          new Case(
              "dataProcess",
              "dataProcessKey",
              "urn:li:dataProcess:(process,airflow,%s)",
              "{\"name\":\"process\",\"orchestrator\":\"airflow\",\"origin\":\"%s\"}"),
          new Case(
              "container",
              "containerProperties",
              "urn:li:container:c1",
              "{\"name\":\"c1\",\"customProperties\":{},\"env\":\"%s\"}"),
          new Case(
              "dataFlow",
              "dataFlowInfo",
              "urn:li:dataFlow:(airflow,flow,prod)",
              "{\"name\":\"flow\",\"customProperties\":{},\"env\":\"%s\"}"),
          new Case(
              "dataJob",
              "dataJobInfo",
              "urn:li:dataJob:(urn:li:dataFlow:(airflow,flow,prod),job)",
              "{\"name\":\"job\",\"type\":{\"string\":\"COMMAND\"},\"customProperties\":{},"
                  + "\"env\":\"%s\"}"),
          new Case(
              "dataPlatformInstance",
              "icebergWarehouseInfo",
              "urn:li:dataPlatformInstance:(urn:li:dataPlatform:iceberg,wh)",
              "{\"dataRoot\":\"s3://bucket/wh\",\"clientId\":\"urn:li:dataHubSecret:id\","
                  + "\"clientSecret\":\"urn:li:dataHubSecret:secret\",\"region\":\"us-east-1\","
                  + "\"env\":\"%s\"}"));

  /** Built-in envs used for search document snapshots; every symbol is covered by M1. */
  private static final List<String> DOCUMENT_ENVS = List.of("PROD", "DEV", "CERT");

  private OperationContext opContext;
  private EntityRegistry entityRegistry;

  @BeforeClass
  public void setUp() {
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    entityRegistry = opContext.getEntityRegistry();
  }

  /** M1: stored JSON for every built-in env round-trips unchanged and passes validation. */
  @Test
  public void testStoredAspectsRoundTripForEveryBuiltInEnv() {
    for (Case c : CASES) {
      for (FabricType env : builtInEnvs()) {
        String stored = String.format(c.aspectJson, env.name());
        RecordTemplate aspect = toAspect(c, stored);

        RecordTemplateValidator.validate(
            aspect, result -> fail(c.aspectName + "/" + env + ": " + result.getMessages()));
        // JsonNode.equals is order-insensitive for objects, unlike TestNG's Iterable comparison.
        assertTrue(
            readTree(RecordUtils.toJsonString(aspect)).equals(readTree(stored)),
            c.aspectName + "/" + env + " did not round-trip unchanged");
        assertEquals(aspect.data().get(c.envField()), env.name());
      }
    }
  }

  /** The schema itself no longer limits the field to the enum; FabricTypeValidator does that. */
  @Test
  public void testSchemaAcceptsNonEnumEnv() {
    for (Case c : CASES) {
      RecordTemplate aspect = toAspect(c, String.format(c.aspectJson, "MY_ENV"));
      RecordTemplateValidator.validate(
          aspect, result -> fail(c.aspectName + ": " + result.getMessages()));
    }
  }

  /**
   * Each env / origin field still has a mapping an enum field produced. A synthetic index whose
   * only field is that mapping does not reindex. Searchable fields other than env / origin are
   * ignored.
   */
  @Test
  public void testEnvFieldMappingsDoNotForceReindex() throws Exception {
    List<JsonNode> allowed = new ArrayList<>();
    readFixture().get("fieldMappings").forEach(allowed::add);
    Map<String, JsonNode> found = new TreeMap<>();
    buildMappings()
        .fields()
        .forEachRemaining(index -> collectEnvFields(index.getKey(), "", index.getValue(), found));

    List<String> problems = new ArrayList<>();
    for (String required : REQUIRED_ENV_FIELDS) {
      if (!found.containsKey(required)) {
        problems.add("missing " + required);
      }
    }
    for (Map.Entry<String, JsonNode> field : found.entrySet()) {
      Optional<JsonNode> match =
          allowed.stream().filter(shape -> shape.equals(field.getValue())).findFirst();
      if (match.isEmpty()) {
        problems.add(
            field.getKey() + " mapping is not a known env field shape: " + field.getValue());
        continue;
      }
      String path = field.getKey().substring(field.getKey().indexOf(' ') + 1);
      String fieldName = path.substring(path.lastIndexOf('.') + 1);
      Map<String, Object> baseline = Map.of("properties", Map.of(fieldName, toMap(match.get())));
      Map<String, Object> live = Map.of("properties", Map.of(fieldName, toMap(field.getValue())));
      ReindexConfig upgrade =
          ReindexConfig.builder()
              .name("syntheticenvindex_v2")
              .exists(true)
              .currentSettings(Settings.EMPTY)
              .targetSettings(Map.of())
              .currentMappings(baseline)
              .targetMappings(live)
              .enableIndexMappingsReindex(true)
              .enableIndexSettingsReindex(false)
              .enableStructuredPropertiesReindex(false)
              .version("1.0")
              .build();
      if (upgrade.requiresReindex() || upgrade.requiresApplyMappings()) {
        problems.add(field.getKey() + " would be reindexed or remapped on upgrade");
      }
    }
    assertTrue(problems.isEmpty(), String.join("\n", problems));
  }

  private static Map<String, Object> toMap(JsonNode node) {
    return MAPPER.convertValue(node, new TypeReference<Map<String, Object>>() {});
  }

  /** A searchable built-in env is indexed as that same string. A non-searchable env is omitted. */
  @Test
  public void testBuiltInEnvIsIndexedAsItself() throws Exception {
    JsonNode documents = buildDocuments();
    for (Case c : CASES) {
      boolean searchable =
          aspectSpec(c).getSearchableFieldSpecs().stream()
              .anyMatch(field -> field.getPath().getPathComponents().contains(c.envField()));
      for (String env : DOCUMENT_ENVS) {
        JsonNode doc = documents.get(c.aspectName + "/" + env);
        assertNotNull(doc, c.aspectName + "/" + env);
        JsonNode value = doc.path(c.envField());
        if (searchable) {
          assertEquals(value.asText(), env, c.aspectName);
        } else {
          assertTrue(value.isMissingNode(), c.aspectName + " indexes a non-searchable env");
        }
      }
    }
  }

  private JsonNode buildMappings() throws Exception {
    MappingsBuilder builder =
        new DelegatingMappingsBuilder(
            List.of(
                new V2MappingsBuilder(
                    V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION,
                    OpenSearchSearchClientShim.PARTIAL_NGRAM_CONFIG,
                    ESUtils.KEYWORD_MAXLENGTH),
                new MultiEntityMappingsBuilder(
                    V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION,
                    null,
                    ESUtils.KEYWORD_MAXLENGTH,
                    List.of())));
    // V2 indices are per entity. V3 indices share one mapping across entity types, so a single
    // representative covers them (asserted below).
    Set<String> kept =
        CASES.stream()
            .map(c -> c.entityName.toLowerCase() + "index_v2")
            .collect(Collectors.toCollection(TreeSet::new));
    kept.add(V3_REPRESENTATIVE_INDEX);
    Map<String, Object> byIndex = new TreeMap<>();
    Set<String> v3Mappings = new TreeSet<>();
    for (MappingsBuilder.IndexMapping mapping : builder.getIndexMappings(opContext)) {
      String index = mapping.getIndexName();
      if (index.endsWith("_v3")) {
        v3Mappings.add(MAPPER.writeValueAsString(mapping.getMappings()));
      }
      if (kept.contains(index)) {
        byIndex.put(index, mapping.getMappings());
      }
    }
    assertEquals(v3Mappings.size(), 1, "V3 indices no longer share one mapping");
    assertEquals(byIndex.keySet(), kept);
    return MAPPER.readTree(MAPPER.writeValueAsString(byIndex));
  }

  private JsonNode buildDocuments() throws Exception {
    SearchDocumentTransformer transformer =
        new SearchDocumentTransformer(1000, 1000, 1000, false, ESUtils.KEYWORD_MAXLENGTH);
    AuditStamp stamp =
        new AuditStamp().setTime(0L).setActor(UrnUtils.getUrn("urn:li:corpuser:datahub"));
    ObjectNode documents = MAPPER.createObjectNode();
    for (Case c : CASES) {
      AspectSpec spec = aspectSpec(c);
      for (String env : DOCUMENT_ENVS) {
        Urn urn = UrnUtils.getUrn(c.urn.contains("%s") ? String.format(c.urn, env) : c.urn);
        RecordTemplate aspect = toAspect(c, String.format(c.aspectJson, env));
        Optional<ObjectNode> doc =
            transformer.transformAspect(opContext, urn, aspect, spec, false, stamp);
        assertTrue(doc.isPresent(), c.aspectName + "/" + env + " produced no document");
        documents.set(c.aspectName + "/" + env, doc.get());
      }
    }
    return MAPPER.readTree(MAPPER.writeValueAsString(documents));
  }

  private RecordTemplate toAspect(Case c, String json) {
    return RecordUtils.toRecordTemplate(
        aspectSpec(c).getDataTemplateClass().asSubclass(RecordTemplate.class), json);
  }

  private AspectSpec aspectSpec(Case c) {
    AspectSpec spec = entityRegistry.getEntitySpec(c.entityName).getAspectSpec(c.aspectName);
    assertNotNull(spec, c.entityName + "/" + c.aspectName);
    return spec;
  }

  private static List<FabricType> builtInEnvs() {
    return Arrays.stream(FabricType.values()).filter(v -> v != FabricType.$UNKNOWN).toList();
  }

  private JsonNode readFixture() throws Exception {
    try (InputStream in = getClass().getResourceAsStream(ENV_FIELD_MAPPINGS)) {
      assertNotNull(in, "missing " + ENV_FIELD_MAPPINGS);
      return MAPPER.readTree(in);
    }
  }

  private static JsonNode readTree(String json) {
    try {
      return MAPPER.readTree(json);
    } catch (Exception e) {
      throw new IllegalArgumentException(json, e);
    }
  }

  private static void collectEnvFields(
      String index, String path, JsonNode node, Map<String, JsonNode> out) {
    if (!node.isObject()) {
      return;
    }
    node.fields()
        .forEachRemaining(
            entry -> {
              String childPath = path.isEmpty() ? entry.getKey() : path + "." + entry.getKey();
              JsonNode child = entry.getValue();
              if (("env".equals(entry.getKey()) || "origin".equals(entry.getKey()))
                  && child.isObject()
                  && child.has("type")) {
                out.put(index + " " + childPath, child);
              }
              collectEnvFields(index, childPath, child, out);
            });
  }

  private record Case(String entityName, String aspectName, String urn, String aspectJson) {
    String envField() {
      return aspectJson.contains("\"origin\"") ? "origin" : "env";
    }
  }
}
