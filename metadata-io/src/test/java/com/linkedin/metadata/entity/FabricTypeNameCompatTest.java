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
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
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
 * FabricTypeName} string typeref needs no data migration: stored aspects, search mappings and
 * search documents for built-in envs are unchanged.
 *
 * <p>The mapping and document checks compare against a snapshot captured on master before the
 * change. This class only uses APIs that exist on both sides, so it can regenerate that snapshot:
 * run it on master with {@code FABRIC_TYPE_COMPAT_WRITE_SNAPSHOT=<path>} in the environment (or the
 * {@code fabricTypeCompat.writeSnapshot} system property) and check the file in as {@value
 * #SNAPSHOT_RESOURCE}.
 */
public class FabricTypeNameCompatTest {

  private static final ObjectMapper MAPPER =
      new ObjectMapper().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
  private static final String SNAPSHOT_RESOURCE = "/fabric-type-compat/master-snapshot.json";
  private static final String WRITE_SNAPSHOT_PROPERTY = "fabricTypeCompat.writeSnapshot";
  private static final String WRITE_SNAPSHOT_ENV = "FABRIC_TYPE_COMPAT_WRITE_SNAPSHOT";
  private static final String V3_REPRESENTATIVE_INDEX = "datasetindex_v3";

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
   * M2: mappings of every existing index that holds an env / origin field, and of the shared V3
   * mapping, are identical to master, so an upgrade neither reindexes nor updates mappings.
   * customFabricType only adds its own new V2 index.
   */
  @Test
  public void testIndexMappingsUnchangedFromMaster() throws Exception {
    JsonNode expected = readSnapshot().get("mappings");
    JsonNode actual = buildMappings();

    List<String> problems = new ArrayList<>();
    for (Iterator<String> it = expected.fieldNames(); it.hasNext(); ) {
      String index = it.next();
      if (!actual.has(index)) {
        problems.add("missing index " + index);
        continue;
      }
      Map<String, String> before = flatten(expected.get(index));
      Map<String, String> after = flatten(actual.get(index));
      before.forEach(
          (path, value) -> {
            if (!value.equals(after.get(path))) {
              problems.add(index + " " + path + ": " + value + " -> " + after.get(path));
            }
          });
      after.keySet().stream()
          .filter(path -> !before.containsKey(path))
          .forEach(path -> problems.add(index + " new mapping " + path));

      ReindexConfig upgrade =
          ReindexConfig.builder()
              .name(index)
              .exists(true)
              .currentSettings(Settings.EMPTY)
              .targetSettings(Map.of())
              .currentMappings(toMap(expected.get(index)))
              .targetMappings(toMap(actual.get(index)))
              .enableIndexMappingsReindex(true)
              .enableIndexSettingsReindex(false)
              .enableStructuredPropertiesReindex(false)
              .version("1.0")
              .build();
      if (upgrade.requiresReindex() || upgrade.requiresApplyMappings()) {
        problems.add(index + " would be reindexed or remapped on upgrade");
      }
    }
    assertTrue(problems.isEmpty(), String.join("\n", problems));
  }

  private static Map<String, Object> toMap(JsonNode node) {
    return MAPPER.convertValue(node, new TypeReference<Map<String, Object>>() {});
  }

  /** M4: search documents for built-in envs are identical to master. */
  @Test
  public void testSearchDocumentsUnchangedFromMaster() throws Exception {
    JsonNode expected = readSnapshot().get("documents");
    JsonNode actual = buildDocuments();
    // JsonNode.equals is order-insensitive for objects, unlike TestNG's Iterable comparison.
    assertTrue(actual.equals(expected), "documents differ:\n" + actual + "\nvs\n" + expected);
  }

  /**
   * Writes the master snapshot when {@value #WRITE_SNAPSHOT_PROPERTY} is set. Run on master only.
   */
  @Test
  public void writeSnapshotWhenRequested() throws Exception {
    String path =
        Optional.ofNullable(System.getProperty(WRITE_SNAPSHOT_PROPERTY))
            .orElse(System.getenv(WRITE_SNAPSHOT_ENV));
    if (path == null) {
      return;
    }
    ObjectNode snapshot = MAPPER.createObjectNode();
    snapshot.set("mappings", buildMappings());
    snapshot.set("documents", buildDocuments());
    MAPPER.writerWithDefaultPrettyPrinter().writeValue(new File(path), snapshot);
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

  private JsonNode readSnapshot() throws Exception {
    try (InputStream in = getClass().getResourceAsStream(SNAPSHOT_RESOURCE)) {
      assertNotNull(in, "missing " + SNAPSHOT_RESOURCE);
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

  /** Flattens a mapping into leaf path -> value, e.g. {@code properties.origin.type -> keyword}. */
  private static Map<String, String> flatten(JsonNode node) {
    Map<String, String> out = new TreeMap<>();
    flatten("", node, out);
    return out;
  }

  private static void flatten(String prefix, JsonNode node, Map<String, String> out) {
    if (node.isObject()) {
      node.fields()
          .forEachRemaining(
              e ->
                  flatten(
                      prefix.isEmpty() ? e.getKey() : prefix + "." + e.getKey(),
                      e.getValue(),
                      out));
    } else {
      out.put(prefix, node.toString());
    }
  }

  private record Case(String entityName, String aspectName, String urn, String aspectJson) {
    String envField() {
      return aspectJson.contains("\"origin\"") ? "origin" : "env";
    }
  }
}
