package io.datahubproject.openlineage.customfacet;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.FabricType;
import com.linkedin.common.GlobalTags;
import com.linkedin.data.template.StringMap;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.converter.OpenLineageToDataHub;
import io.datahubproject.openlineage.dataset.DatahubJob;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class CustomRunFacetProcessorTest {
  private static final URI SPARK_PRODUCER =
      URI.create("https://github.com/OpenLineage/OpenLineage/tree/1.45.0/integration/spark");
  private static final URI AIRFLOW_PRODUCER =
      URI.create("https://github.com/apache/airflow/tree/providers-openlineage/2.3.0");
  private static final URI RUN_FACET_SCHEMA =
      URI.create("https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunFacet");
  private static final CustomRunFacetProcessor PROCESSOR = new CustomRunFacetProcessor();

  @Test
  public void testSparkFixtureProducesExpectedFlowAndJobContributions() {
    CustomRunFacetContributions result =
        PROCESSOR.process(fixture("spark-run-event.json").getRun().getFacets());

    assertEquals(result.flowProperties().get("jobId"), "42");
    assertEquals(result.flowProperties().get("jobDescription"), "daily customer load");
    assertEquals(result.flowProperties().get("spark.master"), "yarn");
    assertTrue(result.flowProperties().get("spark.logicalPlan").contains("Project"));
    assertEquals(result.jobProperties().get("jobId"), "42");
    assertEquals(result.jobProperties().get("spark.app.name"), "CustomerJob");
    assertFalse(result.jobProperties().containsKey("spark.logicalPlan"));
  }

  @Test
  public void testAirflowAndDeprecatedFixtureFacetsCoexist() {
    CustomRunFacetContributions result =
        PROCESSOR.process(fixture("airflow-run-event.json").getRun().getFacets());

    assertEquals(result.flowProperties().get("dag_id"), "customer_pipeline");
    assertEquals(result.flowProperties().get("owner"), "data-platform");
    assertEquals(result.jobProperties().get("task_id"), "load_customer");
    assertEquals(result.jobProperties().get("retries"), "2");
    assertEquals(result.jobProperties().get("legacy_key"), "legacy_value");
    assertTagNames(result.flowTags(), "customer", "production");
  }

  @Test
  public void testProducerAndSchemaNearMissesRemainUnmapped() {
    OpenLineage.RunFacets wrongProducer =
        new OpenLineage(SPARK_PRODUCER)
            .newRunFacetsBuilder()
            .put(
                "spark_jobDetails",
                facet(URI.create("https://example.com/not-spark"), Map.of("jobId", 42)))
            .build();
    assertTrue(PROCESSOR.process(wrongProducer).flowProperties().isEmpty());

    OpenLineage.RunEvent wrongSchema =
        OpenLineageClientUtils.runEventFromJson(
            fixtureJson("spark-run-event.json")
                .replace(
                    "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunFacet",
                    "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/BaseFacet"));
    CustomRunFacetContributions result = PROCESSOR.process(wrongSchema.getRun().getFacets());
    assertFalse(result.flowProperties().containsKey("jobId"));
    assertFalse(result.flowProperties().containsKey("spark.master"));

    for (String producer :
        List.of(
            "https://attacker@github.com/OpenLineage/OpenLineage/tree/1.45.0/integration/spark",
            "https://github.com:8443/OpenLineage/OpenLineage/tree/1.45.0/integration/spark",
            "https://github.com/OpenLineage/OpenLineage/tree/1.45.0/integration/spark?source=x",
            "https://github.com/OpenLineage/OpenLineage/tree/1.45.0/integration/spark#source")) {
      OpenLineage.RunFacets nonCanonicalProducer =
          new OpenLineage(SPARK_PRODUCER)
              .newRunFacetsBuilder()
              .put("spark_jobDetails", facet(URI.create(producer), Map.of("jobId", 42)))
              .build();
      assertTrue(PROCESSOR.process(nonCanonicalProducer).flowProperties().isEmpty(), producer);
    }
  }

  @Test
  public void testMalformedDirectFacetIsSkippedWithoutSuppressingValidSibling() {
    OpenLineage.RunFacet malformed =
        facet(
            SPARK_PRODUCER,
            Map.of("jobId", "not-an-integer", "jobDescription", "must-not-be-partial"));
    OpenLineage.RunFacet valid =
        facet(SPARK_PRODUCER, Map.of("properties", Map.of("spark.master", "local")));
    OpenLineage.RunFacet throwing =
        new OpenLineage.RunFacet() {
          @Override
          public URI get_producer() {
            throw new IllegalStateException("malformed direct caller");
          }

          @Override
          public URI get_schemaURL() {
            return RUN_FACET_SCHEMA;
          }

          @Override
          public Map<String, Object> getAdditionalProperties() {
            return Map.of();
          }
        };
    OpenLineage.RunFacets facets =
        new OpenLineage(SPARK_PRODUCER)
            .newRunFacetsBuilder()
            .put("spark_jobDetails", malformed)
            .put("spark_properties", valid)
            .put("spark.logicalPlan", throwing)
            .build();

    CustomRunFacetContributions result = PROCESSOR.process(facets);
    assertFalse(result.flowProperties().containsKey("jobId"));
    assertFalse(result.flowProperties().containsKey("jobDescription"));
    assertEquals(result.flowProperties().get("spark.master"), "local");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSpecificFacetWinsCompatibilityPropertyCollisions() {
    OpenLineage.RunEvent event = fixture("spark-run-event.json");
    OpenLineage.RunFacet properties =
        event.getRun().getFacets().getAdditionalProperties().get("spark_properties");
    ((Map<String, Object>) properties.getAdditionalProperties().get("properties"))
        .put("jobId", "broad-property-value");

    CustomRunFacetContributions result = PROCESSOR.process(event.getRun().getFacets());
    assertEquals(result.flowProperties().get("jobId"), "42");
    assertEquals(result.jobProperties().get("jobId"), "42");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTypedProcessingEngineWinsRawAndCompatibilityCollisions() throws Exception {
    OpenLineage.RunEvent event = fixture("spark-run-event.json");
    OpenLineage.RunFacet properties =
        event.getRun().getFacets().getAdditionalProperties().get("spark_properties");
    Map<String, Object> propertyValues =
        (Map<String, Object>) properties.getAdditionalProperties().get("properties");
    propertyValues.put(OpenLineageToDataHub.PROCESSING_ENGINE_KEY, "compatibility-value");
    propertyValues.put(OpenLineageToDataHub.PROCESSING_ENGINE_VERSION_KEY, "compatibility-version");
    propertyValues.put(
        OpenLineageToDataHub.OPENLINEAGE_ADAPTER_VERSION_KEY, "compatibility-adapter");
    event
        .getRun()
        .getFacets()
        .getAdditionalProperties()
        .put(
            "processing_engine",
            facet(SPARK_PRODUCER, Map.of("name", "raw-value", "version", "raw-version")));

    DatahubJob converted =
        OpenLineageToDataHub.convertRunEventToJob(
            event,
            DatahubOpenlineageConfig.builder()
                .fabricType(FabricType.PROD)
                .materializeDataset(true)
                .includeSchemaMetadata(true)
                .build());
    List<StringMap> propertiesByTarget =
        List.of(
            converted.getDataFlowInfo().getCustomProperties(),
            converted.getJobInfo().getCustomProperties());
    for (StringMap customProperties : propertiesByTarget) {
      assertEquals(customProperties.get(OpenLineageToDataHub.PROCESSING_ENGINE_KEY), "spark");
      assertEquals(
          customProperties.get(OpenLineageToDataHub.PROCESSING_ENGINE_VERSION_KEY), "3.5.5");
      assertEquals(
          customProperties.get(OpenLineageToDataHub.OPENLINEAGE_ADAPTER_VERSION_KEY), "1.45.0");
      assertFalse(customProperties.containsValue("raw-value"));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAirflowTagsPreserveApostrophesAndRemoveDuplicates() {
    OpenLineage.RunEvent event = fixture("airflow-run-event.json");
    OpenLineage.RunFacet airflow =
        event.getRun().getFacets().getAdditionalProperties().get("airflow");
    ((Map<String, Object>) airflow.getAdditionalProperties().get("dag"))
        .put("tags", "[\"owner's\", \"owner's\", \"z-last\"]");

    GlobalTags tags = PROCESSOR.process(event.getRun().getFacets()).flowTags();
    assertTagNames(tags, "owner's", "z-last");
  }

  @Test
  public void testNullAndUnknownFacetsHaveNoContributions() {
    assertEquals(PROCESSOR.process(null), CustomRunFacetContributions.empty());
    OpenLineage.RunFacets facets =
        new OpenLineage(AIRFLOW_PRODUCER)
            .newRunFacetsBuilder()
            .put("vendor_unknown", facet(AIRFLOW_PRODUCER, Map.of("secret", "value")))
            .build();
    CustomRunFacetContributions result = PROCESSOR.process(facets);
    assertTrue(result.flowProperties().isEmpty());
    assertTrue(result.jobProperties().isEmpty());
    assertNull(result.flowTags());
  }

  private static OpenLineage.RunFacet facet(URI producer, Map<String, Object> properties) {
    OpenLineage.DefaultRunFacet facet = new OpenLineage.DefaultRunFacet(producer);
    facet.getAdditionalProperties().putAll(properties);
    return facet;
  }

  private static OpenLineage.RunEvent fixture(String name) {
    return OpenLineageClientUtils.runEventFromJson(fixtureJson(name));
  }

  private static String fixtureJson(String name) {
    String resource = "/openlineage/compatibility/" + name;
    try (InputStream input = CustomRunFacetProcessorTest.class.getResourceAsStream(resource)) {
      assertNotNull(input, resource);
      return new String(input.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException exception) {
      throw new IllegalStateException("Unable to read " + resource, exception);
    }
  }

  private static void assertTagNames(GlobalTags tags, String... expected) {
    assertNotNull(tags);
    List<String> actual = tags.getTags().stream().map(tag -> tag.getTag().toString()).toList();
    List<String> expectedUrns =
        java.util.Arrays.stream(expected).map(tag -> "urn:li:tag:" + tag).toList();
    assertEquals(actual, expectedUrns);
  }
}
