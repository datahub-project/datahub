package io.datahubproject.openapi.openlineage.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.config.GlobalControllerExceptionHandler;
import io.datahubproject.openapi.config.SpringWebConfig;
import io.datahubproject.openapi.config.TracingInterceptor;
import io.datahubproject.openapi.openlineage.config.DatahubOpenlineageProperties;
import io.datahubproject.openapi.openlineage.config.OpenLineageServletConfig;
import io.datahubproject.openapi.openlineage.exception.OpenLineageControllerExceptionHandler;
import io.datahubproject.openapi.openlineage.mapping.RunEventMapper;
import io.datahubproject.openapi.openlineage.validation.JsonSchemaOpenLineageRequestValidator;
import io.datahubproject.openapi.openlineage.validation.OpenLineageSchemaCatalog;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureWebMvc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@SpringBootTest(classes = SpringWebConfig.class)
@Import({
  SpringWebConfig.class,
  LineageApiImpl.class,
  OpenLineageEventDeserializer.class,
  OpenLineageServletConfig.class,
  OpenLineageSchemaCatalog.class,
  JsonSchemaOpenLineageRequestValidator.class,
  OpenLineageControllerExceptionHandler.class,
  GlobalControllerExceptionHandler.class,
  OpenLineageFixtureCorpusTest.TestConfig.class
})
@AutoConfigureWebMvc
@AutoConfigureMockMvc
public class OpenLineageFixtureCorpusTest extends AbstractTestNGSpringContextTests {
  private static final String FIXTURE_ROOT = "openlineage/fixtures/";
  private static final String SPEC_FIXTURE_ROOT = FIXTURE_ROOT + "openlineage-spec/";
  private static final String ENDPOINT = "/openapi/openlineage/api/v1/lineage";
  private static final String ROOT_SCHEMA = "https://openlineage.io/spec/2-0-2/OpenLineage.json";
  private static final int EXPECTED_EVENT_FIXTURE_COUNT = 90;
  private static final int EXPECTED_FACET_FIXTURE_COUNT = 36;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Autowired private MockMvc mockMvc;
  @Autowired private RunEventMapper.MappingConfig mappingConfig;

  @MockitoBean private EntityServiceImpl entityService;
  @MockitoBean private AuthorizerChain authorizerChain;
  @MockitoBean private TracingInterceptor tracingInterceptor;
  @MockitoBean private ConfigurationProvider configurationProvider;
  @MockitoBean private EntityRegistry entityRegistry;

  @BeforeMethod
  public void setUp() {
    reset(entityService);
    mappingConfig.getDatahubConfig().getUrnAliases().clear();

    Authentication authentication = org.mockito.Mockito.mock(Authentication.class);
    when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "fixture-corpus"));
    AuthenticationContext.setAuthentication(authentication);
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() {
    AuthenticationContext.setAuthentication(null);
  }

  @DataProvider(name = "eventFixtures")
  public static Object[][] eventFixtures() throws IOException {
    return fixtureResources().stream()
        .filter(resource -> !isFacetFixture(resource))
        .map(resource -> new Object[] {new EventFixture(resource)})
        .toArray(Object[][]::new);
  }

  @DataProvider(name = "facetFixtures")
  public static Object[][] facetFixtures() throws IOException {
    return fixtureResources().stream()
        .filter(OpenLineageFixtureCorpusTest::isFacetFixture)
        .map(resource -> new Object[] {new FacetFixture(resource, facetAttachmentPoint(resource))})
        .toArray(Object[][]::new);
  }

  @Test(dataProvider = "eventFixtures")
  public void testEventFixtureAccepted(EventFixture fixture) throws Exception {
    byte[] payload = readResource(fixture.resource());
    JsonNode event = OBJECT_MAPPER.readTree(payload);

    MvcResult result = post(payload);
    assertEquals(
        result.getResponse().getStatus(),
        202,
        fixture + " response body: " + result.getResponse().getContentAsString());

    Set<String> entityTypes = submittedEntityTypes();
    assertFalse(entityTypes.isEmpty(), fixture + " empty MCP batch");
    switch (eventType(event)) {
      case RUN -> assertTrue(
          entityTypes.containsAll(Set.of("dataFlow", "dataJob", "dataProcessInstance")),
          fixture + " missing run entities: " + entityTypes);
      case JOB -> {
        assertTrue(
            entityTypes.containsAll(Set.of("dataFlow", "dataJob")),
            fixture + " missing job entities: " + entityTypes);
        assertFalse(
            entityTypes.contains("dataProcessInstance"),
            fixture + " unexpectedly emitted a process instance");
      }
      case DATASET -> assertEquals(entityTypes, Set.of("dataset"), fixture.toString());
    }
  }

  @Test(dataProvider = "facetFixtures")
  public void testFacetFixtureAccepted(FacetFixture fixture) throws Exception {
    JsonNode facetMap = OBJECT_MAPPER.readTree(readResource(fixture.resource()));
    assertTrue(facetMap.isObject(), fixture + " facet map");
    assertEquals(facetMap.size(), 1, fixture + " facet map size");

    MvcResult result = post(OBJECT_MAPPER.writeValueAsBytes(wrapFacetFixture(fixture, facetMap)));
    assertEquals(
        result.getResponse().getStatus(),
        202,
        fixture + " response body: " + result.getResponse().getContentAsString());
    assertFalse(submittedEntityTypes().isEmpty(), fixture + " empty MCP batch");
  }

  @Test
  public void testFixtureCoverage() throws IOException {
    List<String> resources = fixtureResources();
    assertEquals(resources.size(), EXPECTED_EVENT_FIXTURE_COUNT + EXPECTED_FACET_FIXTURE_COUNT);
    assertEquals(
        resources.stream().filter(OpenLineageFixtureCorpusTest::isFacetFixture).count(),
        EXPECTED_FACET_FIXTURE_COUNT);
    assertEquals(
        resources.stream().filter(resource -> !isFacetFixture(resource)).count(),
        EXPECTED_EVENT_FIXTURE_COUNT);
  }

  private MvcResult post(byte[] payload) throws Exception {
    return mockMvc
        .perform(
            org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post(ENDPOINT)
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
        .andReturn();
  }

  private Set<String> submittedEntityTypes() {
    ArgumentCaptor<AspectsBatch> batchCaptor = ArgumentCaptor.forClass(AspectsBatch.class);
    verify(entityService, times(1))
        .ingestProposal(any(OperationContext.class), batchCaptor.capture(), eq(true));
    return batchCaptor.getValue().getInitialItems().stream()
        .filter(MCPItem.class::isInstance)
        .map(MCPItem.class::cast)
        .map(MCPItem::getMetadataChangeProposal)
        .map(proposal -> proposal.getEntityType())
        .collect(Collectors.toSet());
  }

  private static ObjectNode wrapFacetFixture(FacetFixture fixture, JsonNode facetMap) {
    ObjectNode event = OBJECT_MAPPER.createObjectNode();
    event.put("eventTime", "2026-04-14T10:00:00Z");
    event.put("producer", "https://github.com/OpenLineage/OpenLineage/tree/1.45.0/spec/tests");
    String datasetName = "spec_fixtures." + fixture.id().replace('/', '_');
    switch (fixture.attachmentPoint()) {
      case RUN -> {
        event.put("schemaURL", ROOT_SCHEMA + "#/$defs/RunEvent");
        event.put("eventType", "COMPLETE");
        event
            .putObject("run")
            .put("runId", "123e4567-e89b-12d3-a456-426614174000")
            .set("facets", facetMap.deepCopy());
        event.putObject("job").put("namespace", "openlineage-spec").put("name", "facet.fixture");
        event.putArray("inputs");
        event.putArray("outputs");
      }
      case JOB -> {
        event.put("schemaURL", ROOT_SCHEMA + "#/$defs/JobEvent");
        event
            .putObject("job")
            .put("namespace", "openlineage-spec")
            .put("name", "facet.fixture")
            .set("facets", facetMap.deepCopy());
        event.putArray("inputs");
        event.putArray("outputs");
      }
      case DATASET -> {
        event.put("schemaURL", ROOT_SCHEMA + "#/$defs/DatasetEvent");
        event
            .putObject("dataset")
            .put("namespace", "unknown")
            .put("name", datasetName)
            .set("facets", facetMap.deepCopy());
      }
      case INPUT_DATASET -> {
        event.put("schemaURL", ROOT_SCHEMA + "#/$defs/JobEvent");
        event.putObject("job").put("namespace", "openlineage-spec").put("name", "facet.fixture");
        event
            .putArray("inputs")
            .addObject()
            .put("namespace", "unknown")
            .put("name", datasetName)
            .set("inputFacets", facetMap.deepCopy());
        event.putArray("outputs");
      }
      case OUTPUT_DATASET -> {
        event.put("schemaURL", ROOT_SCHEMA + "#/$defs/JobEvent");
        event.putObject("job").put("namespace", "openlineage-spec").put("name", "facet.fixture");
        event.putArray("inputs");
        event
            .putArray("outputs")
            .addObject()
            .put("namespace", "unknown")
            .put("name", datasetName)
            .set("outputFacets", facetMap.deepCopy());
      }
    }
    return event;
  }

  private static FacetAttachment facetAttachmentPoint(String resource) {
    String[] parts = resource.split("/");
    String schemaName = parts[parts.length - 2];
    if (schemaName.endsWith("InputDatasetFacet")
        || "BaseSubsetDatasetFacet".equals(schemaName)
        || "DataQualityAssertionsDatasetFacet".equals(schemaName)) {
      return FacetAttachment.INPUT_DATASET;
    }
    if (schemaName.endsWith("OutputDatasetFacet")) {
      return FacetAttachment.OUTPUT_DATASET;
    }
    if (schemaName.endsWith("RunFacet")) {
      return FacetAttachment.RUN;
    }
    if (schemaName.endsWith("JobFacet")) {
      return FacetAttachment.JOB;
    }
    return FacetAttachment.DATASET;
  }

  private static boolean isFacetFixture(String resource) {
    return resource.startsWith(SPEC_FIXTURE_ROOT) && !resource.endsWith("/example_full_event.json");
  }

  private static EventType eventType(JsonNode event) {
    if (event.path("run").isObject()) {
      return EventType.RUN;
    }
    if (event.path("dataset").isObject()) {
      return EventType.DATASET;
    }
    return EventType.JOB;
  }

  private static List<String> fixtureResources() throws IOException {
    Path fixtureDirectory = fixtureDirectory();
    try (var paths = Files.walk(fixtureDirectory)) {
      return paths
          .filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().endsWith(".json"))
          .map(
              path ->
                  FIXTURE_ROOT
                      + fixtureDirectory
                          .relativize(path)
                          .toString()
                          .replace(File.separatorChar, '/'))
          .sorted()
          .toList();
    }
  }

  private static byte[] readResource(String resource) throws IOException {
    try (InputStream input =
        OpenLineageFixtureCorpusTest.class.getClassLoader().getResourceAsStream(resource)) {
      assertNotNull(input, "Missing resource " + resource);
      return input.readAllBytes();
    }
  }

  private static Path fixtureDirectory() {
    List<Path> candidates =
        List.of(
            Path.of("src/test/resources/openlineage/fixtures"),
            Path.of("metadata-service/openapi-servlet/src/test/resources/openlineage/fixtures"));
    return candidates.stream()
        .filter(Files::isDirectory)
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("Unable to locate fixture source directory"));
  }

  private record EventFixture(String resource) {
    @Override
    public String toString() {
      return resource;
    }
  }

  private record FacetFixture(String resource, FacetAttachment attachmentPoint) {
    private String id() {
      return resource.substring(SPEC_FIXTURE_ROOT.length(), resource.length() - ".json".length());
    }

    @Override
    public String toString() {
      return resource;
    }
  }

  private enum EventType {
    RUN,
    JOB,
    DATASET
  }

  private enum FacetAttachment {
    RUN,
    JOB,
    DATASET,
    INPUT_DATASET,
    OUTPUT_DATASET
  }

  @TestConfiguration
  public static class TestConfig {
    @Bean(name = "systemOperationContext")
    public OperationContext systemOperationContext() {
      return TestOperationContexts.systemContextNoSearchAuthorization();
    }

    @Bean
    public DatahubOpenlineageProperties datahubOpenlineageProperties() {
      DatahubOpenlineageProperties properties = new DatahubOpenlineageProperties();
      properties.setMaterializeDataset(true);
      properties.setIncludeSchemaMetadata(true);
      properties.setCaptureColumnLevelLineage(true);
      properties.setUsePatch(false);
      return properties;
    }
  }
}
