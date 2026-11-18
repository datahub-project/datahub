package com.linkedin.gms.factory.graphql;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.group.GroupService;
import com.datahub.authentication.invite.InviteTokenService;
import com.datahub.authentication.post.PostService;
import com.datahub.authentication.token.StatefulTokenService;
import com.datahub.authentication.user.NativeUserService;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.datahub.authorization.role.RoleService;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.datahub.graphql.GraphQLEngine;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.plugins.SpringStandardPluginConfiguration;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.gms.factory.search.MappingsBuilderFactory;
import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.incident.IncidentType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.plugins.PluginFactory;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import com.linkedin.metadata.connection.ConnectionService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.versioning.EntityVersioningService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.SiblingGraphService;
import com.linkedin.metadata.ingestion.IngestionCliVersionMatrixService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.recommendation.RecommendationsService;
import com.linkedin.metadata.recommendation.candidatesource.RecentlySearchedSource;
import com.linkedin.metadata.recommendation.candidatesource.RecentlyViewedSource;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SemanticSearchService;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.search.elasticsearch.index.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.semantic.SemanticEntitySearch;
import com.linkedin.metadata.service.*;
import com.linkedin.metadata.service.docimport.DocumentImportService;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.metadata.utils.objectstorage.ObjectStorageClient;
import com.linkedin.metadata.version.GitVersion;
import graphql.ExecutionResult;
import graphql.GraphQLError;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeUtil;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.metadata.services.RestrictedService;
import io.datahubproject.metadata.services.SecretService;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.mockito.Answers;
import org.mockito.Mockito;
import org.mockito.invocation.Invocation;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Authorization sweep over the production GraphQL engine.
 *
 * <p>Builds the real {@code GmsGraphQLEngine} through {@link GraphQLEngineFactory} (backed by
 * mocked storage/services) and invokes every {@code Mutation} field and every {@code Query} field
 * as an authenticated user that holds no privileges at all (the authorizer denies every request,
 * and View Authorization is enabled). Arguments are generated from the schema.
 *
 * <p>Each field must be refused: the response carries an UNAUTHORIZED (403) error and no write
 * reached any mocked storage or service. Fields that are legitimately open (self-service on the
 * caller's own user, public configuration, reads that are filtered rather than refused, ...) and
 * known gaps are listed in {@link #MUTATION_BASELINE} / {@link #QUERY_BASELINE} with the outcome
 * observed today. The baselines may only shrink: a new field that is not refused fails the test,
 * and so does a listed field whose outcome changes (remove it from the baseline when it becomes
 * refused).
 */
@SpringBootTest(
    classes = {
      ConfigurationProvider.class,
      GraphQLEngineFactory.class,
      MappingsBuilderFactory.class,
    })
@ContextConfiguration(classes = GraphQLResolverAuthorizationSweepTest.TestConfig.class)
@TestPropertySource(
    locations = "classpath:/application.yaml",
    properties = {
      // Turn on optional features so their resolvers are wired and swept too.
      "platformAnalytics.enabled=true",
      "featureFlags.businessAttributeEntityEnabled=true",
      "featureFlags.entityVersioning=true",
      "graphQL.concurrency.separateThreadPool=true",
      "LINEAGE_DEFAULT_LAST_DAYS_FILTER=30",
      "authentication.tokenService.signingKey=test-signing-key-for-tests",
      "authentication.tokenService.salt=test-salt-for-tests",
      "spring.main.allow-bean-definition-overriding=true",
    })
public class GraphQLResolverAuthorizationSweepTest extends AbstractTestNGSpringContextTests {

  /** How a field behaved when called by an actor with no privileges. */
  enum Outcome {
    /** UNAUTHORIZED (403) error and nothing written. */
    DENIED,
    /**
     * Nothing written and no 403, but the resolver consulted the authorizer (and was denied), so
     * the result was filtered or redacted rather than refused outright.
     */
    CHECKED,
    /** A state-changing call reached a storage or service mock. */
    WROTE,
    /** Succeeded without ever consulting the authorizer. */
    RETURNED,
    /** Failed with a non-authorization error without ever consulting the authorizer. */
    FAILED;

    /**
     * Mutations must answer 403. Queries may instead consult the authorizer and filter or redact
     * the result (View Authorization), so CHECKED also counts as refused for them. A redacted
     * entity is still a non-null object of the same type, so the returned data cannot tell
     * redaction from a leak; for queries CHECKED therefore only proves the authorizer was asked.
     */
    boolean refused(boolean mutation) {
      return this == DENIED || (!mutation && this == CHECKED);
    }
  }

  /**
   * Mutation fields that are not refused for an actor without privileges, with the outcome observed
   * today. May only shrink: remove an entry when its field becomes refused.
   */
  static final Map<String, Outcome> MUTATION_BASELINE =
      new TreeMap<>(
          Map.ofEntries(
              // OPEN: self-service, acts only on the caller's own user or with a caller-held invite
              // token.
              Map.entry("acceptRole", Outcome.FAILED),
              Map.entry("updateCorpUserLocaleSettings", Outcome.WROTE),
              Map.entry("updateCorpUserViewsSettings", Outcome.WROTE),
              Map.entry("updateUserHomePageSettings", Outcome.WROTE),
              Map.entry("updateUserSetting", Outcome.WROTE),
              // DOWNSTREAM: authorized in a layer this test mocks. PageModuleService /
              // PageTemplateService check scope and creator; logicalParent writes go through
              // LogicalParentAuthorizationValidator; structured property prompt answers go through
              // the aspect privilege validators.
              Map.entry("deletePageModule", Outcome.WROTE),
              Map.entry("deletePageTemplate", Outcome.WROTE),
              Map.entry("linkPhysicalChild", Outcome.WROTE),
              Map.entry("setLogicalParent", Outcome.WROTE),
              Map.entry("submitFormPrompt", Outcome.WROTE),
              Map.entry("unlinkPhysicalChild", Outcome.WROTE),
              // PRECONDITION: the generated arguments fail an existence or input check that runs
              // before the authorization check (the check exists on the success path).
              Map.entry("batchAddToDataProducts", Outcome.FAILED),
              Map.entry("batchRemoveFromDataProducts", Outcome.FAILED),
              Map.entry("batchSetApplication", Outcome.FAILED),
              Map.entry("batchUnsetApplication", Outcome.FAILED),
              Map.entry("createERModelRelationship", Outcome.FAILED),
              Map.entry("deleteView", Outcome.FAILED),
              Map.entry("linkAssetVersion", Outcome.FAILED),
              Map.entry("moveDataProduct", Outcome.FAILED),
              Map.entry("reportAssertionResult", Outcome.FAILED),
              Map.entry("unlinkAssetVersion", Outcome.FAILED),
              Map.entry("updateQuery", Outcome.FAILED),
              Map.entry("updateView", Outcome.FAILED),
              // REFUSED IN PAYLOAD: refuses with success=false in the result instead of an error.
              Map.entry("patchEntities", Outcome.RETURNED),
              Map.entry("patchEntity", Outcome.RETURNED),
              // FOLLOW-UP: baselined, see follow-up.
              Map.entry("batchUpdateStepStates", Outcome.WROTE),
              Map.entry("createDataHubFile", Outcome.FAILED),
              Map.entry("deleteAssertion", Outcome.WROTE),
              Map.entry("deleteQuery", Outcome.WROTE)));

  /**
   * Query fields that are not refused for an actor without privileges, with the outcome observed
   * today. May only shrink: remove an entry when its field becomes refused.
   */
  static final Map<String, Outcome> QUERY_BASELINE =
      new TreeMap<>(
          Map.ofEntries(
              // OPEN: public configuration, or data every authenticated user may read (types that
              // are view-unrestricted by default, the caller's own data, aggregate counts).
              Map.entry("appConfig", Outcome.RETURNED),
              Map.entry("assertion", Outcome.RETURNED),
              Map.entry("bulkEntityDataProducts", Outcome.RETURNED),
              Map.entry("businessAttribute", Outcome.RETURNED),
              Map.entry("connection", Outcome.RETURNED),
              Map.entry("corpGroup", Outcome.RETURNED),
              Map.entry("corpUser", Outcome.RETURNED),
              Map.entry("dataPlatform", Outcome.RETURNED),
              Map.entry("dataPlatformInstance", Outcome.RETURNED),
              Map.entry("docPropagationSettings", Outcome.RETURNED),
              Map.entry("erModelRelationship", Outcome.RETURNED),
              Map.entry("form", Outcome.RETURNED),
              Map.entry("getEntityCounts", Outcome.RETURNED),
              Map.entry("globalHomePageSettings", Outcome.RETURNED),
              Map.entry("globalViewsSettings", Outcome.RETURNED),
              Map.entry("isAnalyticsEnabled", Outcome.RETURNED),
              Map.entry("latestProductUpdate", Outcome.RETURNED),
              Map.entry("listLifecycleStages", Outcome.RETURNED),
              Map.entry("role", Outcome.RETURNED),
              Map.entry("structuredProperty", Outcome.RETURNED),
              Map.entry("versionSet", Outcome.RETURNED),
              // OPEN: search, browse, lineage and listings whose results are view-filtered in the
              // search layer or redacted by the entity mappers (canView), not refused by the
              // resolver. The mocked search returns nothing, so neither is exercised here.
              Map.entry("aggregateAcrossEntities", Outcome.RETURNED),
              Map.entry("autoComplete", Outcome.FAILED),
              Map.entry("autoCompleteForMultiple", Outcome.RETURNED),
              Map.entry("browse", Outcome.FAILED),
              Map.entry("browsePaths", Outcome.FAILED),
              Map.entry("browseV2", Outcome.FAILED),
              Map.entry("getQuickFilters", Outcome.FAILED),
              Map.entry("getRootDataProducts", Outcome.FAILED),
              Map.entry("getRootGlossaryNodes", Outcome.FAILED),
              Map.entry("getRootGlossaryTerms", Outcome.FAILED),
              Map.entry("getRootMetrics", Outcome.FAILED),
              Map.entry("getSemanticModels", Outcome.FAILED),
              Map.entry("listBusinessAttributes", Outcome.FAILED),
              Map.entry("listDataProductAssets", Outcome.RETURNED),
              Map.entry("listDomains", Outcome.FAILED),
              Map.entry("listGlobalViews", Outcome.FAILED),
              Map.entry("listMyViews", Outcome.FAILED),
              Map.entry("listOwnershipTypes", Outcome.FAILED),
              Map.entry("listPosts", Outcome.FAILED),
              Map.entry("listRecommendations", Outcome.RETURNED),
              Map.entry("listRoles", Outcome.FAILED),
              Map.entry("scrollAcrossEntities", Outcome.RETURNED),
              Map.entry("scrollAcrossLineage", Outcome.FAILED),
              Map.entry("search", Outcome.FAILED),
              Map.entry("searchAcrossEntities", Outcome.RETURNED),
              Map.entry("searchAcrossLineage", Outcome.FAILED),
              Map.entry("searchAcrossLineageCounts", Outcome.FAILED),
              Map.entry("semanticSearch", Outcome.FAILED),
              Map.entry("semanticSearchAcrossEntities", Outcome.RETURNED),
              // PRECONDITION: the mapper or resolver checks access, but the mocked data stops it
              // earlier (missing key aspect, versioned read not mocked, object storage not
              // configured). Query.entity only resolves __typename here, so the entity is never
              // loaded.
              Map.entry("domain", Outcome.RETURNED),
              Map.entry("entity", Outcome.RETURNED),
              Map.entry("getPresignedUploadUrl", Outcome.FAILED),
              Map.entry("versionedDataset", Outcome.RETURNED),
              // FOLLOW-UP: baselined, see follow-up.
              Map.entry("batchGetStepStates", Outcome.RETURNED),
              Map.entry("dataProcessInstance", Outcome.RETURNED),
              Map.entry("entityExists", Outcome.RETURNED),
              Map.entry("executionRequest", Outcome.RETURNED),
              Map.entry("getAnalyticsCharts", Outcome.RETURNED),
              Map.entry("getHighlights", Outcome.RETURNED),
              Map.entry("getMetadataAnalyticsCharts", Outcome.RETURNED),
              Map.entry("ingestionSource", Outcome.RETURNED),
              Map.entry("listExecutionRequests", Outcome.FAILED),
              Map.entry("listUsers", Outcome.FAILED),
              Map.entry("tag", Outcome.RETURNED),
              Map.entry("test", Outcome.RETURNED)));

  private static final String MUTATION = "Mutation";

  private static final Urn SWEEP_ACTOR = Urn.createFromTuple("corpuser", "authz-sweep-no-privs");

  /**
   * Mock methods that only read. Every other call on a storage or service mock counts as a write,
   * so a new state-changing method is caught without updating this list.
   */
  private static final Pattern READ_METHOD =
      Pattern.compile(
          "^(get|batchGet|list|search|scroll|browse|autoComplete|aggregate|exists|filter|count"
              + "|find|fetch|is|has|can|supports|estimate|read|lookup|load|describe|validate|verify"
              + "|semanticSearch"
              + "|generate\\w*Urn$|toString$|equals$|hashCode$).*");

  /** Mocks whose calls are bookkeeping rather than state changes. */
  private static final List<Class<?>> IGNORED_MOCKS =
      List.of(MetricUtils.class, EntityRegistry.class);

  // URN hints: first matching keyword (in order) in the argument name, then in the field name.
  private static final List<Map.Entry<String, String>> URN_HINTS =
      List.of(
          Map.entry("glossaryterm", "urn:li:glossaryTerm:sweep"),
          Map.entry("glossarynode", "urn:li:glossaryNode:sweep"),
          Map.entry("parentnode", "urn:li:glossaryNode:sweep"),
          Map.entry("term", "urn:li:glossaryTerm:sweep"),
          Map.entry("dataproduct", "urn:li:dataProduct:sweep"),
          Map.entry("structuredpropert", "urn:li:structuredProperty:sweep"),
          Map.entry("ownershiptype", "urn:li:ownershipType:sweep"),
          Map.entry("ownertype", "urn:li:ownershipType:sweep"),
          Map.entry("businessattribute", "urn:li:businessAttribute:sweep"),
          Map.entry("ingestionsource", "urn:li:dataHubIngestionSource:sweep"),
          Map.entry("ingestion", "urn:li:dataHubIngestionSource:sweep"),
          Map.entry("executionrequest", "urn:li:dataHubExecutionRequest:sweep"),
          Map.entry("secret", "urn:li:dataHubSecret:sweep"),
          Map.entry("policy", "urn:li:dataHubPolicy:sweep"),
          Map.entry("policies", "urn:li:dataHubPolicy:sweep"),
          Map.entry("role", "urn:li:dataHubRole:Reader"),
          Map.entry("view", "urn:li:dataHubView:sweep"),
          Map.entry("post", "urn:li:post:sweep"),
          Map.entry("announcement", "urn:li:post:sweep"),
          Map.entry("query", "urn:li:query:sweep"),
          Map.entry("queries", "urn:li:query:sweep"),
          Map.entry("form", "urn:li:form:sweep"),
          Map.entry("domain", "urn:li:domain:sweep"),
          Map.entry("tag", "urn:li:tag:sweep"),
          Map.entry("group", "urn:li:corpGroup:sweep"),
          Map.entry("member", "urn:li:corpuser:sweep-victim"),
          Map.entry("user", "urn:li:corpuser:sweep-victim"),
          Map.entry("owner", "urn:li:corpuser:sweep-victim"),
          Map.entry("actor", "urn:li:corpuser:sweep-victim"),
          Map.entry("token", "urn:li:dataHubAccessToken:sweep"),
          Map.entry("incident", "urn:li:incident:sweep"),
          Map.entry("assertion", "urn:li:assertion:sweep"),
          Map.entry("connection", "urn:li:dataHubConnection:sweep"),
          Map.entry("pagemodule", "urn:li:dataHubPageModule:sweep"),
          Map.entry("module", "urn:li:dataHubPageModule:sweep"),
          Map.entry("pagetemplate", "urn:li:dataHubPageTemplate:sweep"),
          Map.entry("template", "urn:li:dataHubPageTemplate:sweep"),
          Map.entry("document", "urn:li:document:sweep"),
          Map.entry("application", "urn:li:application:sweep"),
          Map.entry("container", "urn:li:container:sweep"),
          Map.entry("contract", "urn:li:dataContract:sweep"),
          Map.entry("ermodelrelationship", "urn:li:erModelRelationship:sweep"),
          Map.entry("file", "urn:li:dataHubFile:sweep"),
          Map.entry("versionset", "urn:li:versionSet:(sweep,dataset)"),
          Map.entry("platform", "urn:li:dataPlatform:hive"),
          Map.entry("chart", "urn:li:chart:(looker,sweep)"),
          Map.entry("dashboard", "urn:li:dashboard:(looker,sweep)"),
          Map.entry("dataflow", "urn:li:dataFlow:(airflow,sweep,PROD)"),
          Map.entry("datajob", "urn:li:dataJob:(urn:li:dataFlow:(airflow,sweep,PROD),sweep)"),
          Map.entry("notebook", "urn:li:notebook:(querybook,sweep)"),
          Map.entry("test", "urn:li:test:sweep"));

  private static final String DEFAULT_URN = "urn:li:dataset:(urn:li:dataPlatform:hive,sweep,PROD)";

  /** URN argument names that always name the asset being acted on. */
  private static final List<String> RESOURCE_ARG_NAMES =
      List.of(
          "resourceurn",
          "resourceurns",
          "entityurn",
          "entityurns",
          "asseturn",
          "asseturns",
          "targeturn",
          "subjecturn");

  @BeforeTest
  public void setup() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @Autowired
  @Qualifier("graphQLEngine")
  private GraphQLEngine graphQLEngine;

  @Autowired
  @Qualifier("systemOperationContext")
  private OperationContext systemOperationContext;

  @Autowired
  @Qualifier("configurationProvider")
  private ConfigurationProvider configurationProvider;

  // Same backing mocks as GraphQLEngineFactoryTest: everything below the engine is mocked.

  @MockitoBean(answers = Answers.RETURNS_MOCKS)
  @Qualifier("searchClientShim")
  private SearchClientShim<?> elasticClient;

  @MockitoBean(answers = Answers.RETURNS_MOCKS)
  @Qualifier("searchClusterRegistry")
  private SearchClusterRegistry searchClusterRegistry;

  @MockitoBean
  @Qualifier("indexConvention")
  private IndexConvention indexConvention;

  @MockitoBean
  @Qualifier("graphClient")
  private GraphClient graphClient;

  @MockitoBean
  @Qualifier("entityService")
  private EntityService<?> entityService;

  @MockitoBean
  @Qualifier("entitySearchService")
  private EntitySearchService entitySearchService;

  @MockitoBean
  @Qualifier("semanticSearchService")
  private SemanticSearchService semanticSearchService;

  @MockitoBean
  @Qualifier("cachingEntitySearchService")
  private CachingEntitySearchService cachingEntitySearchService;

  @MockitoBean
  @Qualifier("semanticEntitySearchService")
  private SemanticEntitySearch semanticEntitySearchService;

  @MockitoBean
  @Qualifier("graphService")
  private GraphService graphService;

  @MockitoBean
  @Qualifier("siblingGraphService")
  private SiblingGraphService siblingGraphService;

  @MockitoBean
  @Qualifier("timeseriesAspectService")
  private TimeseriesAspectService timeseriesAspectService;

  @MockitoBean
  @Qualifier("recommendationsService")
  private RecommendationsService recommendationsService;

  @MockitoBean
  @Qualifier("dataHubTokenService")
  private StatefulTokenService statefulTokenService;

  @MockitoBean
  @Qualifier("dataHubSecretService")
  private SecretService secretService;

  @MockitoBean
  @Qualifier("gitVersion")
  private GitVersion gitVersion;

  @MockitoBean
  @Qualifier("ingestionCliVersionMatrixService")
  private IngestionCliVersionMatrixService versionMatrixService;

  @MockitoBean
  @Qualifier("timelineService")
  private TimelineService timelineService;

  @MockitoBean
  @Qualifier("nativeUserService")
  private NativeUserService nativeUserService;

  @MockitoBean
  @Qualifier("groupService")
  private GroupService groupService;

  @MockitoBean
  @Qualifier("roleService")
  private RoleService roleService;

  @MockitoBean
  @Qualifier("inviteTokenService")
  private InviteTokenService inviteTokenService;

  @MockitoBean
  @Qualifier("postService")
  private PostService postService;

  @MockitoBean
  @Qualifier("viewService")
  private ViewService viewService;

  @MockitoBean
  @Qualifier("ownerShipTypeService")
  private OwnershipTypeService ownershipTypeService;

  @MockitoBean
  @Qualifier("settingsService")
  private SettingsService settingsService;

  @MockitoBean
  @Qualifier("lineageService")
  private LineageService lineageService;

  @MockitoBean
  @Qualifier("queryService")
  private QueryService queryService;

  @MockitoBean
  @Qualifier("erModelRelationshipService")
  private ERModelRelationshipService erModelRelationshipService;

  @MockitoBean
  @Qualifier("dataProductService")
  private DataProductService dataProductService;

  @MockitoBean
  @Qualifier("applicationService")
  private ApplicationService applicationService;

  @MockitoBean
  @Qualifier("formService")
  private FormService formService;

  @MockitoBean
  @Qualifier("restrictedService")
  private RestrictedService restrictedService;

  @MockitoBean
  @Qualifier("businessAttributeService")
  private BusinessAttributeService businessAttributeService;

  @MockitoBean
  @Qualifier("connectionService")
  private ConnectionService connectionService;

  @MockitoBean
  @Qualifier("assertionService")
  private AssertionService assertionService;

  @MockitoBean
  @Qualifier("entityClient")
  private EntityClient entityClient;

  @MockitoBean
  @Qualifier("systemEntityClient")
  private SystemEntityClient systemEntityClient;

  @MockitoBean
  @Qualifier("objectStorageClient")
  private ObjectStorageClient objectStorageClient;

  @MockitoBean private EntityVersioningService entityVersioningService;

  @MockitoBean private MetricUtils metricUtils;

  @MockitoBean private QueryFilterRewriteChain queryFilterRewriteChain;

  @MockitoBean
  @Qualifier("recentlyViewedCandidateSource")
  private RecentlyViewedSource recentlyViewedSource;

  @MockitoBean
  @Qualifier("recentlySearchedCandidateSource")
  private RecentlySearchedSource recentlySearchedSource;

  @MockitoBean
  @Qualifier("pageTemplateService")
  private PageTemplateService pageTemplateService;

  @MockitoBean
  @Qualifier("pageModuleService")
  private PageModuleService pageModuleService;

  @MockitoBean
  @Qualifier("dataHubFileService")
  private DataHubFileService dataHubFileService;

  @MockitoBean
  @Qualifier("documentImportService")
  private DocumentImportService documentImportService;

  @Test
  public void testMutationsRefuseActorWithoutPrivileges() throws Exception {
    assertMatchesBaseline(MUTATION, sweep(MUTATION, "mutation"), MUTATION_BASELINE);
  }

  @Test
  public void testQueriesRefuseActorWithoutPrivileges() throws Exception {
    assertMatchesBaseline("Query", sweep("Query", "query"), QUERY_BASELINE);
  }

  // ---------------------------------------------------------------------------------------------
  // Sweep

  /** Result of calling one field. */
  record Call(Outcome outcome, String detail, int authorizationChecks) {}

  private Map<String, Call> sweep(String rootTypeName, String operation) throws Exception {
    stubExistence();
    GraphQLObjectType root =
        (GraphQLObjectType) graphQLEngine.getGraphQL().getGraphQLSchema().getType(rootTypeName);
    assertNotNull(root, rootTypeName + " type missing from schema");

    List<Object> mocks = collectMocks();
    Map<String, Call> results = new TreeMap<>();
    for (GraphQLFieldDefinition field : root.getFieldDefinitions()) {
      // First with required arguments only. If that failed before reaching the authorizer, retry
      // with every optional argument populated (some resolvers need them to reach their check).
      // A retry never hides a worse first result: a call that succeeded unchecked stays RETURNED
      // unless the retry writes.
      Call call = invoke(operation, field, false, mocks);
      if (call.outcome() == Outcome.FAILED || call.outcome() == Outcome.RETURNED) {
        Call retry = invoke(operation, field, true, mocks);
        if (retry.outcome() == Outcome.WROTE
            || (call.outcome() == Outcome.FAILED && retry.outcome() != Outcome.FAILED)) {
          call = retry;
        }
      }
      results.put(field.getName(), call);
    }

    // Full table in the test log, to triage baseline changes.
    StringBuilder table = new StringBuilder();
    results.forEach(
        (name, call) ->
            table.append(
                String.format(
                    "%n  %s.%s -> %s (authz checks: %d) %s",
                    rootTypeName,
                    name,
                    call.outcome(),
                    call.authorizationChecks(),
                    call.detail())));
    logger.info("Authorization sweep of " + rootTypeName + ":" + table);
    return results;
  }

  private Call invoke(
      String operation, GraphQLFieldDefinition field, boolean populateOptional, List<Object> mocks)
      throws Exception {
    Map<String, Object> variables = new LinkedHashMap<>();
    List<String> declarations = new ArrayList<>();
    List<String> arguments = new ArrayList<>();
    int i = 0;
    for (var argument : field.getArguments()) {
      boolean required = argument.getType() instanceof GraphQLNonNull;
      if (!required && !populateOptional) {
        continue;
      }
      String variable = "v" + i++;
      declarations.add("$" + variable + ": " + GraphQLTypeUtil.simplePrint(argument.getType()));
      arguments.add(argument.getName() + ": $" + variable);
      variables.put(
          variable,
          value(argument.getType(), argument.getName(), field.getName(), populateOptional, 0));
    }
    String query =
        operation
            + " Sweep"
            + (declarations.isEmpty() ? "" : "(" + String.join(", ", declarations) + ")")
            + " { "
            + field.getName()
            + (arguments.isEmpty() ? "" : "(" + String.join(", ", arguments) + ")")
            + selection(field.getType())
            + " }";

    AtomicInteger checks = new AtomicInteger();
    QueryContext context = denyAllContext(checks);
    Mockito.clearInvocations(mocks.toArray());

    ExecutionResult result = graphQLEngine.execute(query, "Sweep", variables, context);
    // Some resolvers finish work with CompletableFuture.runAsync; let it land in this measurement.
    ForkJoinPool.commonPool().awaitQuiescence(10, TimeUnit.SECONDS);

    List<String> writes = writes(mocks);
    if (!writes.isEmpty()) {
      return new Call(Outcome.WROTE, writes.toString(), checks.get());
    }
    List<GraphQLError> errors = result.getErrors();
    boolean denied =
        errors.stream()
            .anyMatch(
                e ->
                    e.getPath() != null
                        && !e.getPath().isEmpty()
                        && field.getName().equals(e.getPath().get(0))
                        && e.getExtensions() != null
                        && Objects.equals(e.getExtensions().get("code"), 403));
    String detail =
        errors.stream()
            .map(
                e ->
                    (e.getExtensions() == null ? "" : e.getExtensions().get("code") + " ")
                        + e.getMessage())
            .collect(Collectors.joining("; "));
    Outcome outcome;
    if (denied) {
      outcome = Outcome.DENIED;
    } else if (checks.get() > 0) {
      outcome = Outcome.CHECKED;
    } else {
      outcome = errors.isEmpty() ? Outcome.RETURNED : Outcome.FAILED;
    }
    return new Call(outcome, detail, checks.get());
  }

  private static void assertMatchesBaseline(
      String rootTypeName, Map<String, Call> results, Map<String, Outcome> baseline) {
    List<String> problems = new ArrayList<>();
    results.forEach(
        (name, call) -> {
          Outcome expected = baseline.get(name);
          boolean ok =
              expected == null
                  ? call.outcome().refused(MUTATION.equals(rootTypeName))
                  : call.outcome() == expected;
          if (!ok) {
            problems.add(
                String.format(
                    "%s.%s: expected %s but was %s (%s)",
                    rootTypeName,
                    name,
                    expected == null
                        ? (MUTATION.equals(rootTypeName) ? "DENIED" : "DENIED or CHECKED")
                        : expected + " (baselined)",
                    call.outcome(),
                    call.detail()));
          }
        });
    baseline.keySet().stream()
        .filter(name -> !results.containsKey(name))
        .forEach(name -> problems.add(rootTypeName + "." + name + ": baselined but not in schema"));
    assertTrue(
        problems.isEmpty(),
        "Fields not refused for an actor without privileges (full table in the test log).\n"
            + "- Not refused: add an authorization check to the resolver. If the check exists but"
            + " the generated arguments fail before reaching it (FAILED), extend URN_HINTS /"
            + " string() so they reach it, or baseline it under PRECONDITION with the reason.\n"
            + "- Baselined but now refused or removed: delete the baseline entry.\n  "
            + String.join("\n  ", problems));
  }

  // ---------------------------------------------------------------------------------------------
  // Request context: an authenticated user that holds no privileges.

  private QueryContext denyAllContext(AtomicInteger checks) throws Exception {
    Authorizer denyAll =
        new Authorizer() {
          @Override
          public AuthorizationResult authorize(@Nonnull AuthorizationRequest request) {
            checks.incrementAndGet();
            return new AuthorizationResult(
                request, AuthorizationResult.Type.DENY, "authorization sweep: deny all");
          }
        };
    Authentication authentication =
        new Authentication(new Actor(ActorType.USER, SWEEP_ACTOR.getId()), "Bearer sweep");
    OperationContext systemContext =
        systemOperationContext.toBuilder()
            .operationContextConfig(
                systemOperationContext.getOperationContextConfig().toBuilder()
                    .viewAuthorizationConfiguration(
                        ViewAuthorizationConfiguration.builder().enabled(true).build())
                    .build())
            .build(systemOperationContext.getSystemActorContext().getAuthentication(), false);
    // Same session construction as the GraphQL servlet (SpringQueryContext).
    OperationContext sessionContext =
        OperationContext.asSession(
            systemContext, RequestContext.TEST, denyAll, authentication, true);
    DataHubAppConfiguration appConfig = configurationProvider;
    return new QueryContext() {
      @Override
      public boolean isAuthenticated() {
        return true;
      }

      @Override
      public Authentication getAuthentication() {
        return authentication;
      }

      @Override
      public Authorizer getAuthorizer() {
        return denyAll;
      }

      @Override
      public OperationContext getOperationContext() {
        return sessionContext;
      }

      @Override
      public DataHubAppConfiguration getDataHubAppConfig() {
        return appConfig;
      }

      @Override
      public int getMaxParentDepth() {
        return 10;
      }
    };
  }

  // ---------------------------------------------------------------------------------------------
  // Mocks

  /** Every referenced entity exists, so resolvers get past existence checks to authorization. */
  @SuppressWarnings("unchecked")
  private void stubExistence() throws Exception {
    when(metricUtils.getRegistry()).thenReturn(new SimpleMeterRegistry());
    when(graphService.supportsMultiHop()).thenReturn(true);
    // Services that gate on their own existence lookup.
    when(dataProductService.verifyEntityExists(any(), any())).thenReturn(true);
    when(applicationService.verifyEntityExists(any(), any())).thenReturn(true);
    for (EntityClient client : List.of(entityClient, systemEntityClient)) {
      when(client.exists(any(), any(Urn.class))).thenReturn(true);
      when(client.exists(any(), any(Urn.class), any(Boolean.class))).thenReturn(true);
      when(client.filterExistingUrns(any(), any()))
          .thenAnswer(inv -> new HashSet<>((Collection<Urn>) inv.getArgument(1)));
    }
    EntityService<?> service = (EntityService<?>) applicationContext.getBean("entityService");
    // Every requested entity is returned (with no aspects), so reads have something to leak.
    Answer<Object> batchGet =
        inv -> {
          Map<Urn, EntityResponse> responses = new LinkedHashMap<>();
          for (Urn urn : (Collection<Urn>) inv.getArgument(2)) {
            responses.put(urn, entityResponse(urn));
          }
          return responses;
        };
    Answer<Object> get = inv -> entityResponse(inv.getArgument(2));
    for (EntityClient client : List.of(entityClient, systemEntityClient)) {
      doAnswer(batchGet).when(client).batchGetV2(any(), anyString(), anySet(), any());
      doAnswer(batchGet).when(client).batchGetV2(any(), anyString(), anySet(), any(), any());
      doAnswer(get).when(client).getV2(any(), anyString(), any(Urn.class), any());
      doAnswer(get).when(client).getV2(any(), anyString(), any(Urn.class), any(), any());
    }

    // Incident resolvers authorize against the entities on the stored IncidentInfo.
    IncidentInfo incident =
        new IncidentInfo()
            .setType(IncidentType.OPERATIONAL)
            .setEntities(new UrnArray(List.of(UrnUtils.getUrn(DEFAULT_URN))))
            .setStatus(
                new IncidentStatus().setState(IncidentState.ACTIVE).setLastUpdated(auditStamp()))
            .setCreated(auditStamp());
    for (EntityService<?> s : List.of(entityService, service)) {
      when(s.getAspect(any(), any(Urn.class), eq(Constants.INCIDENT_INFO_ASPECT_NAME), anyLong()))
          .thenReturn(incident);
    }

    // Covers every exists(...) overload: the boolean ones answer true, the set ones echo the urns.
    Answer<Object> exists =
        inv ->
            inv.getMethod().getReturnType() == boolean.class
                ? Boolean.TRUE
                : new HashSet<>((Collection<Urn>) inv.getArgument(1));
    for (EntityService<?> s : List.of(entityService, service)) {
      doAnswer(exists).when(s).exists(any(), anyCollection(), any(), anyBoolean(), anyBoolean());
      doAnswer(exists).when(s).exists(any(), any(Urn.class), any(), anyBoolean());
      doAnswer(exists).when(s).exists(any(), anyCollection(), anyBoolean());
      doAnswer(exists).when(s).exists(any(), anyCollection(), anyBoolean(), anyBoolean());
      doAnswer(exists).when(s).exists(any(), anyCollection());
      doAnswer(exists).when(s).exists(any(), any(Urn.class), anyBoolean());
      doAnswer(exists).when(s).exists(any(), any(Urn.class), anyBoolean(), anyBoolean());
      doAnswer(exists).when(s).exists(any(), any(Urn.class));
    }
  }

  private static AuditStamp auditStamp() {
    return new AuditStamp().setActor(SWEEP_ACTOR).setTime(0L);
  }

  private static EntityResponse entityResponse(Urn urn) {
    return new EntityResponse()
        .setUrn(urn)
        .setEntityName(urn.getEntityType())
        .setAspects(new EnvelopedAspectMap());
  }

  private List<Object> collectMocks() {
    List<Object> mocks = new ArrayList<>();
    for (String name : applicationContext.getBeanDefinitionNames()) {
      Object bean;
      try {
        bean = applicationContext.getBean(name);
      } catch (Exception e) {
        continue;
      }
      if (Mockito.mockingDetails(bean).isMock()
          && IGNORED_MOCKS.stream().noneMatch(type -> type.isInstance(bean))
          && mocks.stream().noneMatch(m -> m == bean)) {
        mocks.add(bean);
      }
    }
    return mocks;
  }

  private static List<String> writes(List<Object> mocks) {
    List<String> writes = new ArrayList<>();
    for (Object mock : mocks) {
      for (Invocation invocation : Mockito.mockingDetails(mock).getInvocations()) {
        String method = invocation.getMethod().getName();
        if (!READ_METHOD.matcher(method).matches()) {
          writes.add(invocation.getMethod().getDeclaringClass().getSimpleName() + "." + method);
        }
      }
    }
    return writes;
  }

  // ---------------------------------------------------------------------------------------------
  // Argument generation

  private static String selection(GraphQLType type) {
    GraphQLType unwrapped = GraphQLTypeUtil.unwrapAll(type);
    return GraphQLTypeUtil.isScalar(unwrapped) || GraphQLTypeUtil.isEnum(unwrapped)
        ? ""
        : " { __typename }";
  }

  @Nullable
  private static Object value(
      GraphQLInputType type, String name, String fieldName, boolean populateOptional, int depth) {
    if (type instanceof GraphQLNonNull nonNull) {
      return value(
          (GraphQLInputType) nonNull.getWrappedType(), name, fieldName, populateOptional, depth);
    }
    if (type instanceof GraphQLList list) {
      return List.of(
          value(
              (GraphQLInputType) list.getWrappedType(), name, fieldName, populateOptional, depth));
    }
    if (type instanceof GraphQLEnumType enumType) {
      return enumType.getValues().stream()
          // Prefer the variant that needs the broadest privilege (global over personal scope).
          .filter(v -> v.getName().equals("GLOBAL") || v.getName().equals("DATASET"))
          .findFirst()
          .orElse(enumType.getValues().get(0))
          .getName();
    }
    if (type instanceof GraphQLInputObjectType inputType) {
      Map<String, Object> fields = new LinkedHashMap<>();
      for (GraphQLInputObjectField inputField : inputType.getFieldDefinitions()) {
        boolean required = inputField.getType() instanceof GraphQLNonNull;
        // Optional sub-resource scoping stays empty: a generated value names no real field and is
        // rejected before the authorization check.
        boolean subResource =
            inputField.getName().toLowerCase(Locale.ROOT).startsWith("subresource");
        if (required || (populateOptional && depth < 3 && !subResource)) {
          fields.put(
              inputField.getName(),
              value(
                  inputField.getType(),
                  inputField.getName(),
                  fieldName,
                  populateOptional,
                  depth + 1));
        }
      }
      return fields;
    }
    return scalar((GraphQLScalarType) type, name, fieldName);
  }

  private static Object scalar(GraphQLScalarType type, String name, String fieldName) {
    switch (type.getName()) {
      case "Int":
        return 1;
      case "Long":
        return 1L;
      case "Float":
        return 1.0;
      case "Boolean":
        return false;
      default:
        return string(name, fieldName);
    }
  }

  private static String string(String name, String fieldName) {
    String arg = name.toLowerCase(Locale.ROOT);
    if (arg.contains("urn")) {
      if (RESOURCE_ARG_NAMES.contains(arg)) {
        return DEFAULT_URN;
      }
      String hinted = urnHint(arg.replace("urns", "").replace("urn", ""));
      if (hinted == null && (arg.equals("urn") || arg.equals("urns"))) {
        hinted = urnHint(fieldName.toLowerCase(Locale.ROOT));
      }
      return hinted != null ? hinted : DEFAULT_URN;
    }
    if (arg.equals("entitytype")) {
      return "dataset";
    }
    if (arg.contains("email")) {
      return "sweep@example.com";
    }
    if (arg.contains("url") || arg.contains("link")) {
      return "https://example.com/sweep";
    }
    if (arg.contains("json") || arg.contains("recipe") || arg.contains("config")) {
      return "{}";
    }
    if (arg.contains("timezone")) {
      return "UTC";
    }
    return "sweep";
  }

  @Nullable
  private static String urnHint(String text) {
    if (text.isEmpty()) {
      return null;
    }
    return URN_HINTS.stream()
        .filter(hint -> text.contains(hint.getKey()))
        .map(Map.Entry::getValue)
        .findFirst()
        .orElse(null);
  }

  // ---------------------------------------------------------------------------------------------

  @Configuration
  static class TestConfig {

    @Bean(name = "settingsBuilder")
    @Primary
    public SettingsBuilder settingsBuilder() {
      return Mockito.mock(SettingsBuilder.class);
    }

    @Bean(name = "entityService")
    @Primary
    @SuppressWarnings("unchecked")
    public EntityService<?> entityService() {
      return Mockito.mock(EntityService.class);
    }

    @Bean(name = "entityRegistry")
    @Primary
    public EntityRegistry entityRegistry() {
      EntityRegistry registry = Mockito.mock(EntityRegistry.class);
      Mockito.when(registry.getPluginFactory()).thenReturn(PluginFactory.empty());
      return registry;
    }

    @Bean(name = "baseElasticSearchComponents")
    @Primary
    public BaseElasticSearchComponentsFactory.BaseElasticSearchComponents
        baseElasticSearchComponents() {
      return Mockito.mock(BaseElasticSearchComponentsFactory.BaseElasticSearchComponents.class);
    }

    @Bean
    public SpringStandardPluginConfiguration springStandardPluginConfiguration() {
      return new SpringStandardPluginConfiguration();
    }

    @Bean("systemOperationContext")
    public OperationContext systemOperationContext(MetricUtils metricUtils) {
      OperationContext defaultContext = TestOperationContexts.systemContextNoSearchAuthorization();
      return defaultContext.toBuilder()
          .systemTelemetryContext(
              SystemTelemetryContext.builder()
                  .metricUtils(metricUtils)
                  .tracer(OpenTelemetry.noop().getTracer("authz-sweep"))
                  .build())
          .build(defaultContext.getSystemActorContext().getAuthentication(), false);
    }

    @Bean
    public ObjectMapper objectMapper(
        @Qualifier("systemOperationContext") OperationContext operationContext) {
      return operationContext.getObjectMapper();
    }
  }
}
