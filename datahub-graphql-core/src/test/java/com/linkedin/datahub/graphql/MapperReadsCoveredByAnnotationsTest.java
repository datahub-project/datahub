package com.linkedin.datahub.graphql;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.VersionedUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.util.AspectUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import graphql.Scalars;
import graphql.scalars.ExtendedScalars;
import graphql.schema.Coercing;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import io.datahubproject.metadata.services.SecretService;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * The guard for semantically WRONG annotations, complementing {@link AspectMappingCompletenessTest}
 * (which guards forgotten annotations and unknown aspect names).
 *
 * <p>Every late regression in the aspect-optimization work had the same shape: a mapper reads an
 * aspect that no {@code @aspectMapping} on its GraphQL type mentions ({@code semanticText}, {@code
 * assertionNote}, {@code Document.exists} -> {@code status}, {@code Role.isAssignedToMe} -> {@code
 * actors}). Such an aspect is unreachable under optimization — no selection can ever cause it to be
 * fetched — so whatever the mapper populates from it silently disappears.
 *
 * <p>This test runs every optimized loader against a recording aspect map, captures every aspect
 * name its mapper reads, and asserts each read is reachable: present in the union of the type's
 * schema annotations, its hydration-required aspects, or its key aspect (key aspects are always
 * included by the loaders).
 */
public class MapperReadsCoveredByAnnotationsTest {

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

  /**
   * Reviewed mapper reads that are intentionally not annotation-reachable. Add entries only with a
   * justification; each one is data the optimization can drop.
   */
  private static final Map<String, Set<String>> ALLOWED_EXTRA_READS = Map.of();

  private GraphQLSchema schema;
  private AspectMappingRegistry registry;

  @BeforeClass
  public void buildSchemaAndRegistry() {
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
    GraphQLScalarType longScalar =
        GraphQLScalarType.newScalar()
            .name("Long")
            .coercing((Coercing<Object, Object>) ExtendedScalars.GraphQLLong.getCoercing())
            .build();
    RuntimeWiring wiring =
        RuntimeWiring.newRuntimeWiring()
            .scalar(longScalar)
            .scalar(Scalars.GraphQLString)
            .wiringFactory(new PermissiveWiringFactory())
            .build();
    schema = new SchemaGenerator().makeExecutableSchema(typeRegistry, wiring);
    registry = new AspectMappingRegistry(schema);
  }

  private interface LoaderInvocation {
    void run(EntityClient client, QueryContext context) throws Exception;
  }

  private static final class Case {
    final String typeName;
    final String urn;
    final LoaderInvocation invoke;

    Case(String typeName, String urn, LoaderInvocation invoke) {
      this.typeName = typeName;
      this.urn = urn;
      this.invoke = invoke;
    }
  }

  private List<Case> cases() {
    FeatureFlags flags = mock(FeatureFlags.class);
    when(flags.isSchemaFieldEntityFetchEnabled()).thenReturn(true);
    when(flags.isDataProcessInstanceEntityEnabled()).thenReturn(true);
    SecretService secretService = mock(SecretService.class);

    List<Case> cases = new ArrayList<>();
    cases.add(
        new Case(
            "Application",
            "urn:li:application:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.application.ApplicationType(c)
                    .batchLoad(List.of("urn:li:application:guard"), ctx)));
    cases.add(
        new Case(
            "Assertion",
            "urn:li:assertion:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.assertion.AssertionType(c)
                    .batchLoad(List.of("urn:li:assertion:guard"), ctx)));
    cases.add(
        new Case(
            "AccessTokenMetadata",
            "urn:li:dataHubAccessToken:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.auth.AccessTokenMetadataType(c)
                    .batchLoad(List.of("urn:li:dataHubAccessToken:guard"), ctx)));
    cases.add(
        new Case(
            "BusinessAttribute",
            "urn:li:businessAttribute:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.businessattribute.BusinessAttributeType(c)
                    .batchLoad(List.of("urn:li:businessAttribute:guard"), ctx)));
    cases.add(
        new Case(
            "Chart",
            "urn:li:chart:(looker,guard)",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.chart.ChartType(c)
                    .batchLoad(List.of("urn:li:chart:(looker,guard)"), ctx)));
    cases.add(
        new Case(
            "DataHubConnection",
            "urn:li:dataHubConnection:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.connection.DataHubConnectionType(
                        c, secretService)
                    .batchLoad(List.of("urn:li:dataHubConnection:guard"), ctx)));
    cases.add(
        new Case(
            "Container",
            "urn:li:container:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.container.ContainerType(c)
                    .batchLoad(List.of("urn:li:container:guard"), ctx)));
    cases.add(
        new Case(
            "CorpGroup",
            "urn:li:corpGroup:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.corpgroup.CorpGroupType(c)
                    .batchLoad(List.of("urn:li:corpGroup:guard"), ctx)));
    cases.add(
        new Case(
            "CorpUser",
            "urn:li:corpuser:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.corpuser.CorpUserType(c, flags)
                    .batchLoad(List.of("urn:li:corpuser:guard"), ctx)));
    cases.add(
        new Case(
            "Dashboard",
            "urn:li:dashboard:(looker,guard)",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.dashboard.DashboardType(c)
                    .batchLoad(List.of("urn:li:dashboard:(looker,guard)"), ctx)));
    cases.add(
        new Case(
            "DataContract",
            "urn:li:dataContract:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.datacontract.DataContractType(c)
                    .batchLoad(List.of("urn:li:dataContract:guard"), ctx)));
    cases.add(
        new Case(
            "DataFlow",
            "urn:li:dataFlow:(airflow,guard,PROD)",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.dataflow.DataFlowType(c)
                    .batchLoad(List.of("urn:li:dataFlow:(airflow,guard,PROD)"), ctx)));
    cases.add(
        new Case(
            "DataJob",
            "urn:li:dataJob:(urn:li:dataFlow:(airflow,guard,PROD),task)",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.datajob.DataJobType(c)
                    .batchLoad(
                        List.of("urn:li:dataJob:(urn:li:dataFlow:(airflow,guard,PROD),task)"),
                        ctx)));
    cases.add(
        new Case(
            "DataPlatform",
            "urn:li:dataPlatform:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.dataplatform.DataPlatformType(c)
                    .batchLoad(List.of("urn:li:dataPlatform:guard"), ctx)));
    cases.add(
        new Case(
            "DataPlatformInstance",
            "urn:li:dataPlatformInstance:(urn:li:dataPlatform:guard,instance)",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.dataplatforminstance
                        .DataPlatformInstanceType(c)
                    .batchLoad(
                        List.of("urn:li:dataPlatformInstance:(urn:li:dataPlatform:guard,instance)"),
                        ctx)));
    cases.add(
        new Case(
            "DataProcessInstance",
            "urn:li:dataProcessInstance:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.dataprocessinst.DataProcessInstanceType(
                        c, flags)
                    .batchLoad(List.of("urn:li:dataProcessInstance:guard"), ctx)));
    cases.add(
        new Case(
            "DataProduct",
            "urn:li:dataProduct:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.dataproduct.DataProductType(c)
                    .batchLoad(List.of("urn:li:dataProduct:guard"), ctx)));
    cases.add(
        new Case(
            "Dataset",
            "urn:li:dataset:(urn:li:dataPlatform:hive,guard.table,PROD)",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.dataset.DatasetType(c)
                    .batchLoad(
                        List.of("urn:li:dataset:(urn:li:dataPlatform:hive,guard.table,PROD)"),
                        ctx)));
    cases.add(
        new Case(
            "VersionedDataset",
            "urn:li:dataset:(urn:li:dataPlatform:hive,guard.table,PROD)",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.dataset.VersionedDatasetType(c)
                    .batchLoad(
                        List.of(
                            new VersionedUrn()
                                .setUrn(
                                    UrnUtils.getUrn(
                                        "urn:li:dataset:(urn:li:dataPlatform:hive,guard.table,PROD)"))),
                        ctx)));
    cases.add(
        new Case(
            "DataTypeEntity",
            "urn:li:dataType:datahub.guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.datatype.DataTypeType(c)
                    .batchLoad(List.of("urn:li:dataType:datahub.guard"), ctx)));
    cases.add(
        new Case(
            "Domain",
            "urn:li:domain:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.domain.DomainType(c)
                    .batchLoad(List.of("urn:li:domain:guard"), ctx)));
    cases.add(
        new Case(
            "EntityTypeEntity",
            "urn:li:entityType:datahub.guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.entitytype.EntityTypeType(c)
                    .batchLoad(List.of("urn:li:entityType:datahub.guard"), ctx)));
    cases.add(
        new Case(
            "ERModelRelationship",
            "urn:li:erModelRelationship:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.ermodelrelationship.ERModelRelationshipType(
                        c, flags)
                    .batchLoad(List.of("urn:li:erModelRelationship:guard"), ctx)));
    cases.add(
        new Case(
            "DataHubFile",
            "urn:li:dataHubFile:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.file.DataHubFileType(c)
                    .batchLoad(List.of("urn:li:dataHubFile:guard"), ctx)));
    cases.add(
        new Case(
            "Form",
            "urn:li:form:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.form.FormType(c)
                    .batchLoad(List.of("urn:li:form:guard"), ctx)));
    cases.add(
        new Case(
            "GlossaryNode",
            "urn:li:glossaryNode:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.glossary.GlossaryNodeType(c)
                    .batchLoad(List.of("urn:li:glossaryNode:guard"), ctx)));
    cases.add(
        new Case(
            "GlossaryTerm",
            "urn:li:glossaryTerm:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.glossary.GlossaryTermType(c)
                    .batchLoad(List.of("urn:li:glossaryTerm:guard"), ctx)));
    cases.add(
        new Case(
            "Incident",
            "urn:li:incident:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.incident.IncidentType(c)
                    .batchLoad(List.of("urn:li:incident:guard"), ctx)));
    cases.add(
        new Case(
            "ExecutionRequest",
            "urn:li:dataHubExecutionRequest:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.ingestion.ExecutionRequestType(c)
                    .batchLoad(List.of("urn:li:dataHubExecutionRequest:guard"), ctx)));
    cases.add(
        new Case(
            "Document",
            "urn:li:document:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.knowledge.DocumentType(c)
                    .batchLoad(List.of("urn:li:document:guard"), ctx)));
    cases.add(
        new Case(
            "Metric",
            "urn:li:metric:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.metric.MetricType(c)
                    .batchLoad(List.of("urn:li:metric:guard"), ctx)));
    cases.add(
        new Case(
            "MLFeatureTable",
            "urn:li:mlFeatureTable:(urn:li:dataPlatform:feast,guard)",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.mlmodel.MLFeatureTableType(c)
                    .batchLoad(
                        List.of("urn:li:mlFeatureTable:(urn:li:dataPlatform:feast,guard)"), ctx)));
    cases.add(
        new Case(
            "MLFeature",
            "urn:li:mlFeature:(namespace,guard)",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.mlmodel.MLFeatureType(c)
                    .batchLoad(List.of("urn:li:mlFeature:(namespace,guard)"), ctx)));
    cases.add(
        new Case(
            "MLModelGroup",
            "urn:li:mlModelGroup:(urn:li:dataPlatform:sagemaker,guard,PROD)",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.mlmodel.MLModelGroupType(c)
                    .batchLoad(
                        List.of("urn:li:mlModelGroup:(urn:li:dataPlatform:sagemaker,guard,PROD)"),
                        ctx)));
    cases.add(
        new Case(
            "MLModel",
            "urn:li:mlModel:(urn:li:dataPlatform:sagemaker,guard,PROD)",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.mlmodel.MLModelType(c)
                    .batchLoad(
                        List.of("urn:li:mlModel:(urn:li:dataPlatform:sagemaker,guard,PROD)"),
                        ctx)));
    cases.add(
        new Case(
            "MLPrimaryKey",
            "urn:li:mlPrimaryKey:(namespace,guard)",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.mlmodel.MLPrimaryKeyType(c)
                    .batchLoad(List.of("urn:li:mlPrimaryKey:(namespace,guard)"), ctx)));
    cases.add(
        new Case(
            "DataHubPageModule",
            "urn:li:dataHubPageModule:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.module.PageModuleType(c)
                    .batchLoad(List.of("urn:li:dataHubPageModule:guard"), ctx)));
    cases.add(
        new Case(
            "Notebook",
            "urn:li:notebook:(querybook,guard)",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.notebook.NotebookType(c)
                    .batchLoad(List.of("urn:li:notebook:(querybook,guard)"), ctx)));
    cases.add(
        new Case(
            "OwnershipTypeEntity",
            "urn:li:ownershipType:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.ownership.OwnershipType(c)
                    .batchLoad(List.of("urn:li:ownershipType:guard"), ctx)));
    cases.add(
        new Case(
            "DataHubPolicy",
            "urn:li:dataHubPolicy:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.policy.DataHubPolicyType(c)
                    .batchLoad(List.of("urn:li:dataHubPolicy:guard"), ctx)));
    cases.add(
        new Case(
            "Post",
            "urn:li:post:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.post.PostType(c)
                    .batchLoad(List.of("urn:li:post:guard"), ctx)));
    cases.add(
        new Case(
            "QueryEntity",
            "urn:li:query:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.query.QueryType(c)
                    .batchLoad(List.of("urn:li:query:guard"), ctx)));
    cases.add(
        new Case(
            "DataHubRole",
            "urn:li:dataHubRole:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.role.DataHubRoleType(c)
                    .batchLoad(List.of("urn:li:dataHubRole:guard"), ctx)));
    cases.add(
        new Case(
            "Role",
            "urn:li:role:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.rolemetadata.RoleType(c)
                    .batchLoad(List.of("urn:li:role:guard"), ctx)));
    cases.add(
        new Case(
            "SchemaFieldEntity",
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,guard.table,PROD),col)",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.schemafield.SchemaFieldType(c, flags)
                    .batchLoad(
                        List.of(
                            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,guard.table,PROD),col)"),
                        ctx)));
    cases.add(
        new Case(
            "SemanticModel",
            "urn:li:semanticModel:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.semanticmodel.SemanticModelType(c)
                    .batchLoad(List.of("urn:li:semanticModel:guard"), ctx)));
    cases.add(
        new Case(
            "StructuredPropertyEntity",
            "urn:li:structuredProperty:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.structuredproperty.StructuredPropertyType(c)
                    .batchLoad(List.of("urn:li:structuredProperty:guard"), ctx)));
    cases.add(
        new Case(
            "Tag",
            "urn:li:tag:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.tag.TagType(c)
                    .batchLoad(List.of("urn:li:tag:guard"), ctx)));
    cases.add(
        new Case(
            "DataHubPageTemplate",
            "urn:li:dataHubPageTemplate:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.template.PageTemplateType(c)
                    .batchLoad(List.of("urn:li:dataHubPageTemplate:guard"), ctx)));
    cases.add(
        new Case(
            "Test",
            "urn:li:test:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.test.TestType(c)
                    .batchLoad(List.of("urn:li:test:guard"), ctx)));
    cases.add(
        new Case(
            "VersionSet",
            "urn:li:versionSet:(guard,dataset)",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.versioning.VersionSetType(c)
                    .batchLoad(List.of("urn:li:versionSet:(guard,dataset)"), ctx)));
    cases.add(
        new Case(
            "DataHubView",
            "urn:li:dataHubView:guard",
            (c, ctx) ->
                new com.linkedin.datahub.graphql.types.view.DataHubViewType(c)
                    .batchLoad(List.of("urn:li:dataHubView:guard"), ctx)));
    return cases;
  }

  @Test
  public void testEveryMapperReadIsReachableFromSomeAnnotation() {
    List<String> violations = new ArrayList<>();
    for (Case c : cases()) {
      Set<String> reads;
      try {
        reads = recordMapperReads(c);
      } catch (Exception e) {
        violations.add(c.typeName + ": failed to invoke loader: " + e);
        continue;
      }
      Set<String> reachable = annotationUnion(c.typeName);
      reachable.addAll(AspectUtils.getHydrationRequiredAspects(c.typeName));
      reachable.addAll(ALLOWED_EXTRA_READS.getOrDefault(c.typeName, Set.of()));
      Set<String> unreachable =
          reads.stream()
              // Key aspects are always fetched by the loaders (alwaysInclude), so reading them is
              // safe even when no field maps them.
              .filter(read -> !read.toLowerCase().endsWith("key"))
              .filter(read -> !reachable.contains(read))
              .collect(Collectors.toCollection(TreeSet::new));
      if (!unreachable.isEmpty()) {
        violations.add(c.typeName + " mapper reads unreachable aspects " + unreachable);
      }
    }
    assertTrue(
        violations.isEmpty(),
        "Mappers read aspects that no @aspectMapping on their type can ever cause to be fetched,"
            + " so the data they populate silently disappears under optimization (the"
            + " semanticText/assertionNote/Role.actors bug class): "
            + violations
            + ". Fix the field's @aspectMapping (or HYDRATION_REQUIRED_ASPECTS for"
            + " selection-independent needs); use ALLOWED_EXTRA_READS only with a justification.");
  }

  private Set<String> recordMapperReads(Case c) throws Exception {
    EntityClient client = mock(EntityClient.class);
    EnvelopedAspectMap recordingAspectMap = mock(EnvelopedAspectMap.class);
    Urn urn = UrnUtils.getUrn(c.urn);
    EntityResponse response = mock(EntityResponse.class);
    when(response.getEntityName()).thenReturn(urn.getEntityType());
    when(response.getUrn()).thenReturn(urn);
    when(response.getAspects()).thenReturn(recordingAspectMap);
    when(client.batchGetV2(any(), any(), any(), any())).thenReturn(Map.of(urn, response));

    QueryContext context = getMockAllowContext();
    try {
      c.invoke.run(client, context);
    } catch (RuntimeException e) {
      // Some mappers throw when a required aspect is absent (Incident, DataContract); their reads
      // were recorded before the throw, which is all this test needs.
    }

    return Mockito.mockingDetails(recordingAspectMap).getInvocations().stream()
        .filter(
            invocation ->
                invocation.getMethod().getName().equals("containsKey")
                    || invocation.getMethod().getName().equals("get")
                    || invocation.getMethod().getName().equals("getOrDefault"))
        .filter(invocation -> invocation.getArguments().length > 0)
        .map(invocation -> invocation.getArgument(0))
        .filter(String.class::isInstance)
        .map(String.class::cast)
        .collect(Collectors.toSet());
  }

  /** Union of every {@code @aspectMapping} over all fields of {@code typeName}. */
  private Set<String> annotationUnion(String typeName) {
    GraphQLObjectType type = (GraphQLObjectType) schema.getType(typeName);
    Objects.requireNonNull(type, "GraphQL type not found in schema: " + typeName);
    Set<String> union = new HashSet<>();
    type.getFieldDefinitions()
        .forEach(
            field -> {
              Set<String> aspects =
                  registry.getRequiredAspectsForFieldNames(typeName, Set.of(field.getName()));
              if (aspects != null) {
                union.addAll(aspects);
              }
            });
    return union;
  }

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
