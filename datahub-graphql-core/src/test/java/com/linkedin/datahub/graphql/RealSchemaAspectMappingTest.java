package com.linkedin.datahub.graphql;

import static org.testng.Assert.*;

import graphql.Scalars;
import graphql.scalars.ExtendedScalars;
import graphql.schema.Coercing;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchema;
import graphql.schema.SelectedField;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import graphql.schema.idl.errors.SchemaProblem;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Exercises {@link AspectMappingRegistry} against the real, fully-merged production GraphQL schema
 * (the same set of SDL files the engine loads). This validates the actual {@code @aspectMapping} /
 * {@code @noAspects} annotations across entity types, rather than a hand-written SDL snippet.
 */
public class RealSchemaAspectMappingTest {

  // Mirrors the schema files loaded in GmsGraphQLEngine.configureRuntimeWiring / builder.
  private static final String[] SCHEMA_FILES = {
    "entity.graphql",
    "search.graphql",
    "app.graphql",
    "app.semantic.graphql",
    "auth.graphql",
    "analytics.graphql",
    "recommendation.graphql",
    "ingestion.graphql",
    "timeline.graphql",
    "tests.graphql",
    "step.graphql",
    "lineage.graphql",
    "properties.graphql",
    "forms.graphql",
    "common.graphql",
    "logical.graphql",
    "connection.graphql",
    "assertions.graphql",
    "incident.graphql",
    "contract.graphql",
    "operations.graphql",
    "timeseries.graphql",
    "versioning.graphql",
    "query.graphql",
    "template.graphql",
    "module.graphql",
    "patch.graphql",
    "settings.graphql",
    "files.graphql",
    "documents.graphql",
    "metrics.graphql",
    "runs.graphql",
    "lifecycle.graphql",
    "dataProduct.graphql"
  };

  private AspectMappingRegistry registry;

  @BeforeClass
  public void buildRegistryFromRealSchema() {
    SchemaParser parser = new SchemaParser();
    TypeDefinitionRegistry typeRegistry = new TypeDefinitionRegistry();
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    for (String file : SCHEMA_FILES) {
      try (InputStream is = cl.getResourceAsStream(file)) {
        String sdl =
            IOUtils.toString(Objects.requireNonNull(is, "Missing " + file), StandardCharsets.UTF_8);
        typeRegistry.merge(parser.parse(sdl));
      } catch (Exception e) {
        throw new RuntimeException("Failed to load/parse " + file, e);
      }
    }

    // Custom scalar used across the schema. Directives are declared in entity.graphql itself.
    GraphQLScalarType longScalar =
        GraphQLScalarType.newScalar()
            .name("Long")
            .coercing((Coercing<Object, Object>) ExtendedScalars.GraphQLLong.getCoercing())
            .build();

    RuntimeWiring wiring =
        RuntimeWiring.newRuntimeWiring()
            .scalar(longScalar)
            .scalar(Scalars.GraphQLString)
            // Provide permissive resolvers so interface/union types build without executor wiring.
            .wiringFactory(new PermissiveWiringFactory())
            .build();

    GraphQLSchema schema;
    try {
      schema = new SchemaGenerator().makeExecutableSchema(typeRegistry, wiring);
    } catch (SchemaProblem p) {
      throw new RuntimeException("Failed to build executable schema: " + p.getMessage(), p);
    }
    registry = new AspectMappingRegistry(schema);
  }

  private SelectedField field(String fieldName, String typeName) {
    SelectedField f = Mockito.mock(SelectedField.class);
    Mockito.when(f.getName()).thenReturn(fieldName);
    Mockito.when(f.getObjectTypeNames()).thenReturn(Arrays.asList(typeName));
    return f;
  }

  /**
   * IsAssignedToMeResolver reads role.getActors() from the parent Role, which RoleMapper only
   * populates when the actors aspect is fetched. The Access Management query selects isAssignedToMe
   * without actors, so the field must pull the aspect itself or assigned users see
   * isAssignedToMe=false.
   */
  @Test
  public void testRoleIsAssignedToMeMapsToActors() {
    assertEquals(
        registry.getRequiredAspects("Role", List.of(field("isAssignedToMe", "Role"))),
        Set.of("actors"));
  }

  /** DocumentMapper sets exists from the status aspect (soft-delete flag). */
  @Test
  public void testDocumentExistsMapsToStatus() {
    Set<String> aspects =
        registry.getRequiredAspects("Document", List.of(field("exists", "Document")));
    assertTrue(aspects.contains("status"), "Got: " + aspects);
  }

  /** AssertionMapper prefers the standalone assertionNote aspect for info.note. */
  @Test
  public void testAssertionInfoMapsToInfoAndNote() {
    assertEquals(
        registry.getRequiredAspects("Assertion", List.of(field("info", "Assertion"))),
        Set.of("assertionInfo", "assertionNote"));
  }

  @Test
  public void testDocumentInfoMapsToInfoAndSemanticText() {
    // DocumentMapper folds the standalone semanticText aspect into info.contents.semanticText, so
    // the info field must pull both documentInfo and semanticText.
    assertEquals(
        registry.getRequiredAspects("Document", List.of(field("info", "Document"))),
        Set.of("documentInfo", "semanticText"));
  }

  /**
   * lastIngested is computed from the system metadata of every fetched aspect
   * (SystemMetadataUtils.getLastIngestedTime), so any narrower fetch silently changes its value.
   * Selecting it must force the fetch-all fallback via @fetchAllAspects.
   */
  @Test
  public void testLastIngestedForcesFetchAll() {
    assertEquals(
        registry.getRequiredAspects("Dataset", List.of(field("lastIngested", "Dataset"))), null);
    assertEquals(
        registry.getRequiredAspects(
            "Dataset", List.of(field("name", "Dataset"), field("lastIngested", "Dataset"))),
        null,
        "fetch-all must dominate the rest of the selection");
    assertEquals(
        registry.getRequiredAspects("Document", List.of(field("lastIngested", "Document"))), null);
  }

  @Test
  public void testDatasetMinimalSelection() {
    Set<String> aspects =
        registry.getRequiredAspects(
            "Dataset", List.of(field("urn", "Dataset"), field("name", "Dataset")));
    assertNotNull(aspects);
    assertTrue(aspects.contains("datasetKey"), "name maps to datasetKey. Got: " + aspects);
    assertTrue(aspects.contains("datasetProperties"), "Got: " + aspects);
    // Optimization must not fetch unrelated aspects.
    assertFalse(aspects.contains("upstreamLineage"), "Got: " + aspects);
    assertFalse(aspects.contains("globalTags"), "Got: " + aspects);
  }

  @Test
  public void testDatasetOwnershipAndTags() {
    Set<String> aspects =
        registry.getRequiredAspects(
            "Dataset", List.of(field("ownership", "Dataset"), field("tags", "Dataset")));
    assertNotNull(aspects);
    assertEquals(aspects, Set.of("ownership", "globalTags"));
  }

  @Test
  public void testChartMinimalSelection() {
    Set<String> aspects =
        registry.getRequiredAspects(
            "Chart", List.of(field("properties", "Chart"), field("tool", "Chart")));
    assertNotNull(aspects);
    assertEquals(aspects, Set.of("chartInfo", "chartKey"));
  }

  @Test
  public void testDashboardMinimalSelection() {
    Set<String> aspects =
        registry.getRequiredAspects("Dashboard", List.of(field("properties", "Dashboard")));
    assertNotNull(aspects);
    assertEquals(aspects, Set.of("dashboardInfo"));
  }

  @Test
  public void testCorpUserStatusUsesCorpUserStatusAspect() {
    Set<String> aspects =
        registry.getRequiredAspects(
            "CorpUser", List.of(field("status", "CorpUser"), field("username", "CorpUser")));
    assertNotNull(aspects);
    assertTrue(
        aspects.contains("corpUserStatus"), "CorpUser.status != status aspect. Got: " + aspects);
    assertTrue(aspects.contains("corpUserKey"), "Got: " + aspects);
    assertFalse(aspects.contains("status"), "Should not fetch generic status. Got: " + aspects);
  }

  @Test
  public void testDomainMinimalSelection() {
    Set<String> aspects =
        registry.getRequiredAspects(
            "Domain", List.of(field("properties", "Domain"), field("ownership", "Domain")));
    assertNotNull(aspects);
    assertEquals(aspects, Set.of("domainProperties", "ownership"));
  }

  @Test
  public void testContainerMinimalSelection() {
    Set<String> aspects =
        registry.getRequiredAspects("Container", List.of(field("properties", "Container")));
    assertNotNull(aspects);
    assertEquals(aspects, Set.of("containerProperties"));
  }

  @Test
  public void testGlossaryTermMinimalSelection() {
    Set<String> aspects =
        registry.getRequiredAspects("GlossaryTerm", List.of(field("properties", "GlossaryTerm")));
    assertNotNull(aspects);
    assertEquals(aspects, Set.of("glossaryTermInfo"));
  }

  @Test
  public void testMlModelMetricsUsesMetricsAspect() {
    Set<String> aspects =
        registry.getRequiredAspects("MLModel", List.of(field("metrics", "MLModel")));
    assertNotNull(aspects);
    assertEquals(aspects, Set.of("mlModelMetrics"));
  }

  @Test
  public void testNestedTypeFilteringIgnoresForeignFields() {
    // A Dataset "name" field plus a SearchResult "name" field: only the Dataset one counts.
    Set<String> aspects =
        registry.getRequiredAspects(
            "Dataset", List.of(field("name", "Dataset"), field("entity", "SearchResult")));
    assertNotNull(aspects);
    assertTrue(aspects.contains("datasetKey"));
    assertTrue(aspects.contains("datasetProperties"));
  }

  @Test
  public void testExtensionFieldsAnnotated() {
    // Fields declared via `extend type Dataset` must also be mapped (no fallback).
    Set<String> aspects =
        registry.getRequiredAspects(
            "Dataset",
            List.of(
                field("versionProperties", "Dataset"),
                field("logicalParent", "Dataset"),
                field("settings", "Dataset")));
    assertNotNull(aspects, "extend-type fields must be annotated (else null/fallback)");
    assertTrue(aspects.contains("versionProperties"));
    assertTrue(aspects.contains("logicalParent"));
    assertTrue(aspects.contains("assetSettings"));
  }

  @Test
  public void testNoAspectsFieldContributesNothing() {
    Set<String> aspects =
        registry.getRequiredAspects("Dataset", List.of(field("lineage", "Dataset")));
    assertNotNull(aspects);
    assertTrue(aspects.isEmpty(), "lineage is @noAspects. Got: " + aspects);
  }

  @Test
  public void testDataHubViewFieldsMapToViewInfo() {
    Set<String> aspects =
        registry.getRequiredAspects(
            "DataHubView",
            List.of(
                field("viewType", "DataHubView"),
                field("name", "DataHubView"),
                field("description", "DataHubView"),
                field("definition", "DataHubView")));
    assertNotNull(aspects);
    assertEquals(aspects, Set.of("dataHubViewInfo"));
  }

  @Test
  public void testTagDescriptionAndDeprecationAspects() {
    Set<String> aspects =
        registry.getRequiredAspects(
            "Tag", List.of(field("description", "Tag"), field("deprecation", "Tag")));
    assertNotNull(aspects);
    assertEquals(aspects, Set.of("tagProperties", "deprecation"));
  }

  @Test
  public void testAccessTokenMetadataFieldsMapToTokenInfo() {
    Set<String> aspects =
        registry.getRequiredAspects(
            "AccessTokenMetadata",
            List.of(
                field("description", "AccessTokenMetadata"),
                field("actorUrn", "AccessTokenMetadata"),
                field("ownerUrn", "AccessTokenMetadata"),
                field("createdAt", "AccessTokenMetadata"),
                field("expiresAt", "AccessTokenMetadata")));
    assertNotNull(aspects);
    assertEquals(aspects, Set.of("dataHubAccessTokenInfo"));
  }

  @Test
  public void testMlTopLevelFieldsMapToPropertiesAspects() {
    assertEquals(
        registry.getRequiredAspects(
            "MLFeature",
            List.of(field("description", "MLFeature"), field("dataType", "MLFeature"))),
        Set.of("mlFeatureProperties"));
    assertEquals(
        registry.getRequiredAspects(
            "MLPrimaryKey",
            List.of(
                field("description", "MLPrimaryKey"),
                field("dataType", "MLPrimaryKey"),
                field("primaryKeyProperties", "MLPrimaryKey"))),
        Set.of("mlPrimaryKeyProperties"));
    assertEquals(
        registry.getRequiredAspects(
            "MLFeatureTable",
            List.of(
                field("description", "MLFeatureTable"),
                field("featureTableProperties", "MLFeatureTable"))),
        Set.of("mlFeatureTableProperties"));
    assertEquals(
        registry.getRequiredAspects("MLModel", List.of(field("description", "MLModel"))),
        Set.of("mlModelProperties"));
    assertEquals(
        registry.getRequiredAspects("MLModelGroup", List.of(field("description", "MLModelGroup"))),
        Set.of("mlModelGroupProperties"));
  }

  @Test
  public void testPolicyRoleIncidentFieldsMapToInfoAspects() {
    assertEquals(
        registry.getRequiredAspects(
            "DataHubPolicy",
            List.of(
                field("policyType", "DataHubPolicy"),
                field("name", "DataHubPolicy"),
                field("description", "DataHubPolicy"),
                field("actors", "DataHubPolicy"))),
        Set.of("dataHubPolicyInfo"));
    assertEquals(
        registry.getRequiredAspects(
            "DataHubRole",
            List.of(field("name", "DataHubRole"), field("description", "DataHubRole"))),
        Set.of("dataHubRoleInfo"));
    assertEquals(
        registry.getRequiredAspects(
            "Incident",
            List.of(
                field("title", "Incident"),
                field("description", "Incident"),
                field("status", "Incident"),
                field("created", "Incident"))),
        Set.of("incidentInfo"));
  }

  @Test
  public void testPostAndDpiAndMetricKeyMappings() {
    assertEquals(
        registry.getRequiredAspects(
            "Post",
            List.of(
                field("postType", "Post"),
                field("content", "Post"),
                field("lastModified", "Post"))),
        Set.of("postInfo"));
    assertEquals(
        registry.getRequiredAspects(
            "DataProcessInstance",
            List.of(
                field("name", "DataProcessInstance"),
                field("created", "DataProcessInstance"),
                field("externalUrl", "DataProcessInstance"))),
        Set.of("dataProcessInstanceProperties"));
    assertEquals(
        registry.getRequiredAspects(
            "Metric",
            List.of(
                field("platform", "Metric"),
                field("path", "Metric"),
                field("id", "Metric"),
                field("semanticModel", "Metric"))),
        Set.of("metricKey", "metricInfo"));
    assertEquals(
        registry.getRequiredAspects(
            "SemanticModel",
            List.of(
                field("platform", "SemanticModel"),
                field("info", "SemanticModel"),
                field("documentation", "SemanticModel"))),
        Set.of("semanticModelKey", "semanticModelInfo", "documentation"));
  }

  @Test
  public void testAssertionDatasetMapsToAssertionInfo() {
    assertEquals(
        registry.getRequiredAspects("Assertion", List.of(field("dataset", "Assertion"))),
        Set.of("assertionInfo"));
  }

  /** Minimal wiring factory that lets interface/union types compile without runtime resolvers. */
  private static final class PermissiveWiringFactory implements graphql.schema.idl.WiringFactory {
    @Override
    public boolean providesTypeResolver(graphql.schema.idl.InterfaceWiringEnvironment environment) {
      return true;
    }

    @Override
    public graphql.schema.TypeResolver getTypeResolver(
        graphql.schema.idl.InterfaceWiringEnvironment environment) {
      return env -> null;
    }

    @Override
    public boolean providesTypeResolver(graphql.schema.idl.UnionWiringEnvironment environment) {
      return true;
    }

    @Override
    public graphql.schema.TypeResolver getTypeResolver(
        graphql.schema.idl.UnionWiringEnvironment environment) {
      return env -> null;
    }
  }
}
