package com.linkedin.datahub.graphql;

import com.datahub.authentication.token.TokenService;
import com.datahub.authorization.AuthorizationConfiguration;
import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.analytics.resolver.AnalyticsChartTypeResolver;
import com.linkedin.datahub.graphql.analytics.resolver.GetChartsResolver;
import com.linkedin.datahub.graphql.analytics.resolver.GetHighlightsResolver;
import com.linkedin.datahub.graphql.analytics.resolver.GetMetadataAnalyticsResolver;
import com.linkedin.datahub.graphql.analytics.resolver.IsAnalyticsEnabledResolver;
import com.linkedin.datahub.graphql.analytics.service.AnalyticsService;
import com.linkedin.datahub.graphql.generated.ActorFilter;
import com.linkedin.datahub.graphql.generated.AggregationMetadata;
import com.linkedin.datahub.graphql.generated.Aspect;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.AutoCompleteResultForEntity;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.BrowseResults;
import com.linkedin.datahub.graphql.generated.Chart;
import com.linkedin.datahub.graphql.generated.ChartInfo;
import com.linkedin.datahub.graphql.generated.Container;
import com.linkedin.datahub.graphql.generated.CorpGroupInfo;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.CorpUserInfo;
import com.linkedin.datahub.graphql.generated.Dashboard;
import com.linkedin.datahub.graphql.generated.DashboardInfo;
import com.linkedin.datahub.graphql.generated.DataFlow;
import com.linkedin.datahub.graphql.generated.DataJob;
import com.linkedin.datahub.graphql.generated.DataJobInputOutput;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.EntityRelationship;
import com.linkedin.datahub.graphql.generated.EntityRelationshipLegacy;
import com.linkedin.datahub.graphql.generated.ForeignKeyConstraint;
import com.linkedin.datahub.graphql.generated.InstitutionalMemoryMetadata;
import com.linkedin.datahub.graphql.generated.LineageRelationship;
import com.linkedin.datahub.graphql.generated.ListDomainsResult;
import com.linkedin.datahub.graphql.generated.MLFeature;
import com.linkedin.datahub.graphql.generated.MLFeatureProperties;
import com.linkedin.datahub.graphql.generated.MLFeatureTable;
import com.linkedin.datahub.graphql.generated.MLFeatureTableProperties;
import com.linkedin.datahub.graphql.generated.MLModel;
import com.linkedin.datahub.graphql.generated.MLModelGroup;
import com.linkedin.datahub.graphql.generated.MLModelProperties;
import com.linkedin.datahub.graphql.generated.MLPrimaryKey;
import com.linkedin.datahub.graphql.generated.MLPrimaryKeyProperties;
import com.linkedin.datahub.graphql.generated.Notebook;
import com.linkedin.datahub.graphql.generated.Owner;
import com.linkedin.datahub.graphql.generated.PolicyMatchCriterionValue;
import com.linkedin.datahub.graphql.generated.RecommendationContent;
import com.linkedin.datahub.graphql.generated.SearchAcrossLineageResult;
import com.linkedin.datahub.graphql.generated.SearchResult;
import com.linkedin.datahub.graphql.generated.UsageQueryResult;
import com.linkedin.datahub.graphql.generated.UserUsageCounts;
import com.linkedin.datahub.graphql.generated.VisualConfiguration;
import com.linkedin.datahub.graphql.resolvers.AuthenticatedResolver;
import com.linkedin.datahub.graphql.resolvers.MeResolver;
import com.linkedin.datahub.graphql.resolvers.assertion.AssertionRunEventResolver;
import com.linkedin.datahub.graphql.resolvers.assertion.DeleteAssertionResolver;
import com.linkedin.datahub.graphql.resolvers.assertion.EntityAssertionsResolver;
import com.linkedin.datahub.graphql.resolvers.auth.GetAccessTokenResolver;
import com.linkedin.datahub.graphql.resolvers.browse.BrowsePathsResolver;
import com.linkedin.datahub.graphql.resolvers.browse.BrowseResolver;
import com.linkedin.datahub.graphql.resolvers.config.AppConfigResolver;
import com.linkedin.datahub.graphql.resolvers.container.ContainerEntitiesResolver;
import com.linkedin.datahub.graphql.resolvers.dataset.DatasetHealthResolver;
import com.linkedin.datahub.graphql.resolvers.deprecation.UpdateDeprecationResolver;
import com.linkedin.datahub.graphql.resolvers.domain.CreateDomainResolver;
import com.linkedin.datahub.graphql.resolvers.domain.DomainEntitiesResolver;
import com.linkedin.datahub.graphql.resolvers.domain.ListDomainsResolver;
import com.linkedin.datahub.graphql.resolvers.domain.SetDomainResolver;
import com.linkedin.datahub.graphql.resolvers.domain.UnsetDomainResolver;
import com.linkedin.datahub.graphql.resolvers.group.AddGroupMembersResolver;
import com.linkedin.datahub.graphql.resolvers.group.CreateGroupResolver;
import com.linkedin.datahub.graphql.resolvers.group.EntityCountsResolver;
import com.linkedin.datahub.graphql.resolvers.group.ListGroupsResolver;
import com.linkedin.datahub.graphql.resolvers.group.RemoveGroupMembersResolver;
import com.linkedin.datahub.graphql.resolvers.group.RemoveGroupResolver;
import com.linkedin.datahub.graphql.resolvers.jobs.EntityRunsResolver;
import com.linkedin.datahub.graphql.resolvers.jobs.DataJobRunsResolver;
import com.linkedin.datahub.graphql.resolvers.user.UpdateUserStatusResolver;
import com.linkedin.datahub.graphql.resolvers.policy.GetGrantedPrivilegesResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.execution.CancelIngestionExecutionRequestResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.execution.CreateIngestionExecutionRequestResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.execution.GetIngestionExecutionRequestResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.execution.IngestionSourceExecutionRequestsResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.secret.CreateSecretResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.secret.DeleteSecretResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.secret.GetSecretValuesResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.secret.ListSecretsResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.source.DeleteIngestionSourceResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.source.GetIngestionSourceResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.source.ListIngestionSourcesResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.source.UpsertIngestionSourceResolver;
import com.linkedin.datahub.graphql.resolvers.load.AspectResolver;
import com.linkedin.datahub.graphql.resolvers.load.EntityLineageResultResolver;
import com.linkedin.datahub.graphql.resolvers.load.EntityRelationshipsResultResolver;
import com.linkedin.datahub.graphql.resolvers.load.EntityTypeBatchResolver;
import com.linkedin.datahub.graphql.resolvers.load.EntityTypeResolver;
import com.linkedin.datahub.graphql.resolvers.load.LoadableTypeBatchResolver;
import com.linkedin.datahub.graphql.resolvers.load.LoadableTypeResolver;
import com.linkedin.datahub.graphql.resolvers.load.OwnerTypeResolver;
import com.linkedin.datahub.graphql.resolvers.load.TimeSeriesAspectResolver;
import com.linkedin.datahub.graphql.resolvers.load.UsageTypeResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.AddLinkResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.AddOwnerResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.AddTagResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.AddTermResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.MutableTypeResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.RemoveLinkResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.RemoveOwnerResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.RemoveTagResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.RemoveTermResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.UpdateDescriptionResolver;
import com.linkedin.datahub.graphql.resolvers.policy.DeletePolicyResolver;
import com.linkedin.datahub.graphql.resolvers.policy.ListPoliciesResolver;
import com.linkedin.datahub.graphql.resolvers.policy.UpsertPolicyResolver;
import com.linkedin.datahub.graphql.resolvers.recommendation.ListRecommendationsResolver;
import com.linkedin.datahub.graphql.resolvers.search.AutoCompleteForMultipleResolver;
import com.linkedin.datahub.graphql.resolvers.search.AutoCompleteResolver;
import com.linkedin.datahub.graphql.resolvers.search.SearchAcrossEntitiesResolver;
import com.linkedin.datahub.graphql.resolvers.search.SearchAcrossLineageResolver;
import com.linkedin.datahub.graphql.resolvers.search.SearchResolver;
import com.linkedin.datahub.graphql.resolvers.tag.SetTagColorResolver;
import com.linkedin.datahub.graphql.resolvers.type.AspectInterfaceTypeResolver;
import com.linkedin.datahub.graphql.resolvers.type.EntityInterfaceTypeResolver;
import com.linkedin.datahub.graphql.resolvers.type.HyperParameterValueTypeResolver;
import com.linkedin.datahub.graphql.resolvers.type.PlatformSchemaUnionTypeResolver;
import com.linkedin.datahub.graphql.resolvers.type.ResultsTypeResolver;
import com.linkedin.datahub.graphql.resolvers.type.TimeSeriesAspectInterfaceTypeResolver;
import com.linkedin.datahub.graphql.resolvers.user.ListUsersResolver;
import com.linkedin.datahub.graphql.resolvers.user.RemoveUserResolver;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.dataprocessinst.mappers.DataProcessInstanceRunEventMapper;
import com.linkedin.datahub.graphql.types.EntityType;
import com.linkedin.datahub.graphql.types.LoadableType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.aspect.AspectType;
import com.linkedin.datahub.graphql.types.assertion.AssertionType;
import com.linkedin.datahub.graphql.types.chart.ChartType;
import com.linkedin.datahub.graphql.types.common.mappers.OperationMapper;
import com.linkedin.datahub.graphql.types.container.ContainerType;
import com.linkedin.datahub.graphql.types.corpgroup.CorpGroupType;
import com.linkedin.datahub.graphql.types.corpuser.CorpUserType;
import com.linkedin.datahub.graphql.types.dashboard.DashboardType;
import com.linkedin.datahub.graphql.types.notebook.NotebookType;
import com.linkedin.datahub.graphql.types.dataflow.DataFlowType;
import com.linkedin.datahub.graphql.types.datajob.DataJobType;
import com.linkedin.datahub.graphql.types.dataplatform.DataPlatformType;
import com.linkedin.datahub.graphql.types.dataset.DatasetType;
import com.linkedin.datahub.graphql.types.dataset.mappers.DatasetProfileMapper;
import com.linkedin.datahub.graphql.types.domain.DomainType;
import com.linkedin.datahub.graphql.types.glossary.GlossaryTermType;
import com.linkedin.datahub.graphql.types.mlmodel.MLFeatureTableType;
import com.linkedin.datahub.graphql.types.mlmodel.MLFeatureType;
import com.linkedin.datahub.graphql.types.mlmodel.MLModelGroupType;
import com.linkedin.datahub.graphql.types.mlmodel.MLModelType;
import com.linkedin.datahub.graphql.types.mlmodel.MLPrimaryKeyType;
import com.linkedin.datahub.graphql.types.tag.TagType;
import com.linkedin.datahub.graphql.types.usage.UsageType;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.recommendation.RecommendationsService;
import com.linkedin.metadata.secret.SecretService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.usage.UsageClient;
import graphql.execution.DataFetcherResult;
import graphql.schema.idl.RuntimeWiring;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.dataloader.BatchLoaderContextProvider;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderOptions;

import static com.linkedin.datahub.graphql.Constants.ANALYTICS_SCHEMA_FILE;
import static com.linkedin.datahub.graphql.Constants.APP_SCHEMA_FILE;
import static com.linkedin.datahub.graphql.Constants.AUTH_SCHEMA_FILE;
import static com.linkedin.datahub.graphql.Constants.GMS_SCHEMA_FILE;
import static com.linkedin.datahub.graphql.Constants.INGESTION_SCHEMA_FILE;
import static com.linkedin.datahub.graphql.Constants.RECOMMENDATIONS_SCHEMA_FILE;
import static com.linkedin.datahub.graphql.Constants.SEARCH_SCHEMA_FILE;
import static com.linkedin.datahub.graphql.Constants.URN_FIELD_NAME;
import static com.linkedin.metadata.Constants.*;
import static graphql.Scalars.GraphQLLong;


/**
 * A {@link GraphQLEngine} configured to provide access to the entities and aspects on the the GMS graph.
 */
@Slf4j
public class GmsGraphQLEngine {

    private final EntityClient entityClient;
    private final GraphClient graphClient;
    private final UsageClient usageClient;

    private final EntityService entityService;
    private final AnalyticsService analyticsService;
    private final RecommendationsService recommendationsService;
    private final EntityRegistry entityRegistry;
    private final TokenService tokenService;
    private final SecretService secretService;
    private final GitVersion gitVersion;
    private final boolean supportsImpactAnalysis;
    private final TimeseriesAspectService timeseriesAspectService;

    private final IngestionConfiguration ingestionConfiguration;
    private final AuthorizationConfiguration authorizationConfiguration;
    private final VisualConfiguration visualConfiguration;

    private final DatasetType datasetType;
    private final CorpUserType corpUserType;
    private final CorpGroupType corpGroupType;
    private final ChartType chartType;
    private final DashboardType dashboardType;
    private final DataPlatformType dataPlatformType;
    private final TagType tagType;
    private final MLModelType mlModelType;
    private final MLModelGroupType mlModelGroupType;
    private final MLFeatureType mlFeatureType;
    private final MLFeatureTableType mlFeatureTableType;
    private final MLPrimaryKeyType mlPrimaryKeyType;
    private final DataFlowType dataFlowType;
    private final DataJobType dataJobType;
    private final GlossaryTermType glossaryTermType;
    private final AspectType aspectType;
    private final UsageType usageType;
    private final ContainerType containerType;
    private final DomainType domainType;
    private final NotebookType notebookType;
    private final AssertionType assertionType;


    /**
     * Configures the graph objects that can be fetched primary key.
     */
    public final List<EntityType<?>> entityTypes;

    /**
     * Configures all graph objects
     */
    public final List<LoadableType<?>> loadableTypes;

    /**
     * Configures the graph objects for owner
     */
    public final List<LoadableType<?>> ownerTypes;

    /**
     * Configures the graph objects that can be searched.
     */
    public final List<SearchableEntityType<?>> searchableTypes;

    /**
     * Configures the graph objects that can be browsed.
     */
    public final List<BrowsableEntityType<?>> browsableTypes;

    @Deprecated
    public GmsGraphQLEngine() {
        this(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            false,
            null);
    }

    public GmsGraphQLEngine(
        final EntityClient entityClient,
        final GraphClient graphClient,
        final UsageClient usageClient,
        final AnalyticsService analyticsService,
        final EntityService entityService,
        final RecommendationsService recommendationsService,
        final TokenService tokenService,
        final TimeseriesAspectService timeseriesAspectService,
        final EntityRegistry entityRegistry,
        final SecretService secretService,
        final IngestionConfiguration ingestionConfiguration,
        final AuthorizationConfiguration authorizationConfiguration,
        final GitVersion gitVersion,
        final boolean supportsImpactAnalysis,
        final VisualConfiguration visualConfiguration
        ) {

        this.entityClient = entityClient;
        this.graphClient = graphClient;
        this.usageClient = usageClient;

        this.analyticsService = analyticsService;
        this.entityService = entityService;
        this.recommendationsService = recommendationsService;
        this.tokenService = tokenService;
        this.secretService = secretService;
        this.entityRegistry = entityRegistry;
        this.gitVersion = gitVersion;
        this.supportsImpactAnalysis = supportsImpactAnalysis;
        this.timeseriesAspectService = timeseriesAspectService;

        this.ingestionConfiguration = Objects.requireNonNull(ingestionConfiguration);
        this.authorizationConfiguration = Objects.requireNonNull(authorizationConfiguration);
        this.visualConfiguration = visualConfiguration;

        this.datasetType = new DatasetType(entityClient);
        this.corpUserType = new CorpUserType(entityClient);
        this.corpGroupType = new CorpGroupType(entityClient);
        this.chartType = new ChartType(entityClient);
        this.dashboardType = new DashboardType(entityClient);
        this.dataPlatformType = new DataPlatformType(entityClient);
        this.tagType = new TagType(entityClient);
        this.mlModelType = new MLModelType(entityClient);
        this.mlModelGroupType = new MLModelGroupType(entityClient);
        this.mlFeatureType = new MLFeatureType(entityClient);
        this.mlFeatureTableType = new MLFeatureTableType(entityClient);
        this.mlPrimaryKeyType = new MLPrimaryKeyType(entityClient);
        this.dataFlowType = new DataFlowType(entityClient);
        this.dataJobType = new DataJobType(entityClient);
        this.glossaryTermType = new GlossaryTermType(entityClient);
        this.aspectType = new AspectType(entityClient);
        this.usageType = new UsageType(this.usageClient);
        this.containerType = new ContainerType(entityClient);
        this.domainType = new DomainType(entityClient);
        this.notebookType = new NotebookType(entityClient);
        this.assertionType = new AssertionType(entityClient);

        // Init Lists
        this.entityTypes = ImmutableList.of(
            datasetType,
            corpUserType,
            corpGroupType,
            dataPlatformType,
            chartType,
            dashboardType,
            tagType,
            mlModelType,
            mlModelGroupType,
            mlFeatureType,
            mlFeatureTableType,
            mlPrimaryKeyType,
            dataFlowType,
            dataJobType,
            glossaryTermType,
            containerType,
            notebookType,
            domainType,
            assertionType
        );
        this.loadableTypes = new ArrayList<>(entityTypes);
        this.ownerTypes = ImmutableList.of(corpUserType, corpGroupType);
        this.searchableTypes = loadableTypes.stream()
            .filter(type -> (type instanceof SearchableEntityType<?>))
            .map(type -> (SearchableEntityType<?>) type)
            .collect(Collectors.toList());
        this.browsableTypes = loadableTypes.stream()
            .filter(type -> (type instanceof BrowsableEntityType<?>))
            .map(type -> (BrowsableEntityType<?>) type)
            .collect(Collectors.toList());
    }

    public static String entitySchema() {
        String defaultSchemaString;
        try {
            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(GMS_SCHEMA_FILE);
            defaultSchemaString = IOUtils.toString(is, StandardCharsets.UTF_8);
            is.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find GraphQL Schema with name " + GMS_SCHEMA_FILE, e);
        }
        return defaultSchemaString;
    }

    public static String searchSchema() {
        String defaultSchemaString;
        try {
            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(SEARCH_SCHEMA_FILE);
            defaultSchemaString = IOUtils.toString(is, StandardCharsets.UTF_8);
            is.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find GraphQL Schema with name " + SEARCH_SCHEMA_FILE, e);
        }
        return defaultSchemaString;
    }

    public static String appSchema() {
        String defaultSchemaString;
        try {
            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(APP_SCHEMA_FILE);
            defaultSchemaString = IOUtils.toString(is, StandardCharsets.UTF_8);
            is.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find GraphQL Schema with name " + APP_SCHEMA_FILE, e);
        }
        return defaultSchemaString;
    }

    public static String authSchema() {
        String defaultSchemaString;
        try {
            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(AUTH_SCHEMA_FILE);
            defaultSchemaString = IOUtils.toString(is, StandardCharsets.UTF_8);
            is.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find GraphQL Schema with name " + AUTH_SCHEMA_FILE, e);
        }
        return defaultSchemaString;
    }

    public static String analyticsSchema() {
        String analyticsSchemaString;
        try {
            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(ANALYTICS_SCHEMA_FILE);
            analyticsSchemaString = IOUtils.toString(is, StandardCharsets.UTF_8);
            is.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find GraphQL Schema with name " + ANALYTICS_SCHEMA_FILE, e);
        }
        return analyticsSchemaString;
    }

    public static String recommendationsSchema() {
        String recommendationsSchemaString;
        try {
            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(RECOMMENDATIONS_SCHEMA_FILE);
            recommendationsSchemaString = IOUtils.toString(is, StandardCharsets.UTF_8);
            is.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find GraphQL Schema with name " + RECOMMENDATIONS_SCHEMA_FILE, e);
        }
        return recommendationsSchemaString;
    }

    public static String ingestionSchema() {
        String ingestionSchema;
        try {
            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(INGESTION_SCHEMA_FILE);
            ingestionSchema = IOUtils.toString(is, StandardCharsets.UTF_8);
            is.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find GraphQL Schema with name " + INGESTION_SCHEMA_FILE, e);
        }
        return ingestionSchema;
    }

    /**
     * Returns a {@link Supplier} responsible for creating a new {@link DataLoader} from
     * a {@link LoadableType}.
     */
    public Map<String, Function<QueryContext, DataLoader<?, ?>>> loaderSuppliers(final List<LoadableType<?>> loadableTypes) {
        return loadableTypes
            .stream()
            .collect(Collectors.toMap(
                    LoadableType::name,
                    (graphType) -> (context) -> createDataLoader(graphType, context)
            ));
    }

    public void configureRuntimeWiring(final RuntimeWiring.Builder builder) {
        configureQueryResolvers(builder);
        configureMutationResolvers(builder);
        configureGenericEntityResolvers(builder);
        configureDatasetResolvers(builder);
        configureCorpUserResolvers(builder);
        configureCorpGroupResolvers(builder);
        configureDashboardResolvers(builder);
        configureNotebookResolvers(builder);
        configureChartResolvers(builder);
        configureTypeResolvers(builder);
        configureTypeExtensions(builder);
        configureTagAssociationResolver(builder);
        configureDataJobResolvers(builder);
        configureDataFlowResolvers(builder);
        configureMLFeatureTableResolvers(builder);
        configureGlossaryRelationshipResolvers(builder);
        configureIngestionSourceResolvers(builder);
        configureAnalyticsResolvers(builder);
        configureContainerResolvers(builder);
        configureGlossaryTermResolvers(builder);
        configureDomainResolvers(builder);
        configureAssertionResolvers(builder);
        configurePolicyResolvers(builder);
        configureDataProcessInstanceResolvers(builder);
    }

    public GraphQLEngine.Builder builder() {
        return GraphQLEngine.builder()
            .addSchema(entitySchema())
            .addSchema(searchSchema())
            .addSchema(appSchema())
            .addSchema(authSchema())
            .addSchema(analyticsSchema())
            .addSchema(recommendationsSchema())
            .addSchema(ingestionSchema())
            .addDataLoaders(loaderSuppliers(loadableTypes))
            .addDataLoader("Aspect", (context) -> createAspectLoader(context))
            .addDataLoader("UsageQueryResult", (context) -> createUsageLoader(context))
            .configureRuntimeWiring(this::configureRuntimeWiring);
    }

    private void configureAnalyticsResolvers(final RuntimeWiring.Builder builder) {
        final boolean isAnalyticsEnabled = analyticsService != null;
        builder.type("Query", typeWiring -> typeWiring.dataFetcher("isAnalyticsEnabled", new IsAnalyticsEnabledResolver(isAnalyticsEnabled)))
            .type("AnalyticsChart", typeWiring -> typeWiring.typeResolver(new AnalyticsChartTypeResolver()));
        if (isAnalyticsEnabled) {
            builder.type("Query", typeWiring -> typeWiring.dataFetcher("getAnalyticsCharts",
                new GetChartsResolver(analyticsService, entityClient))
                .dataFetcher("getHighlights", new GetHighlightsResolver(analyticsService))
                .dataFetcher("getMetadataAnalyticsCharts", new GetMetadataAnalyticsResolver(entityClient)));
        }
    }

    private void configureContainerResolvers(final RuntimeWiring.Builder builder) {
        builder
            .type("Container", typeWiring -> typeWiring
                .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient))
                .dataFetcher("entities", new ContainerEntitiesResolver(entityClient))
                .dataFetcher("domain", new LoadableTypeResolver<>(domainType, (env) -> {
                    final Container container = env.getSource();
                    return container.getDomain() != null ? container.getDomain().getUrn() : null;
                }))
                .dataFetcher("platform",
                    new LoadableTypeResolver<>(dataPlatformType,
                        (env) -> ((Container) env.getSource()).getPlatform().getUrn()))
                .dataFetcher("container",
                    new LoadableTypeResolver<>(containerType,
                        (env) -> {
                            final Container container = env.getSource();
                            return container.getContainer() != null ? container.getContainer().getUrn() : null;
                        })
                )
            );
    }

    private void configureQueryResolvers(final RuntimeWiring.Builder builder) {
        builder.type("Query", typeWiring -> typeWiring
            .dataFetcher("appConfig",
                new AppConfigResolver(gitVersion, analyticsService != null,
                    this.ingestionConfiguration,
                    this.authorizationConfiguration,
                    supportsImpactAnalysis, this.visualConfiguration))
            .dataFetcher("me", new AuthenticatedResolver<>(
                    new MeResolver(this.entityClient)))
            .dataFetcher("search", new AuthenticatedResolver<>(
                    new SearchResolver(this.entityClient)))
            .dataFetcher("searchAcrossEntities",
                new SearchAcrossEntitiesResolver(this.entityClient))
            .dataFetcher("searchAcrossLineage",
                new SearchAcrossLineageResolver(this.entityClient))
            .dataFetcher("autoComplete", new AuthenticatedResolver<>(
                    new AutoCompleteResolver(searchableTypes)))
            .dataFetcher("autoCompleteForMultiple", new AuthenticatedResolver<>(
                    new AutoCompleteForMultipleResolver(searchableTypes)))
            .dataFetcher("browse", new AuthenticatedResolver<>(
                    new BrowseResolver(browsableTypes)))
            .dataFetcher("browsePaths", new AuthenticatedResolver<>(
                    new BrowsePathsResolver(browsableTypes)))
            .dataFetcher("dataset", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(datasetType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("notebook", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(notebookType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("corpUser", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(corpUserType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("corpGroup", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(corpGroupType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("dashboard", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(dashboardType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("chart", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(chartType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("tag", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(tagType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("dataFlow", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(dataFlowType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("dataJob", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(dataJobType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("glossaryTerm", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(glossaryTermType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("domain",
                new LoadableTypeResolver<>(domainType,
                    (env) -> env.getArgument(URN_FIELD_NAME)))
            .dataFetcher("dataPlatform",
                new LoadableTypeResolver<>(dataPlatformType,
                    (env) -> env.getArgument(URN_FIELD_NAME)))
            .dataFetcher("mlFeatureTable", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(mlFeatureTableType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("mlFeature", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(mlFeatureType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("mlPrimaryKey", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(mlPrimaryKeyType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("mlModel", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(mlModelType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("mlModelGroup", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(mlModelGroupType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("assertion", new AuthenticatedResolver<>(
                new LoadableTypeResolver<>(assertionType,
                    (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("listPolicies",
                new ListPoliciesResolver(this.entityClient))
            .dataFetcher("getGrantedPrivileges", new GetGrantedPrivilegesResolver())
            .dataFetcher("listUsers",
                new ListUsersResolver(this.entityClient))
            .dataFetcher("listGroups",
                new ListGroupsResolver(this.entityClient))
            .dataFetcher("listRecommendations",
                new ListRecommendationsResolver(recommendationsService))
            .dataFetcher("getEntityCounts",
                new EntityCountsResolver(this.entityClient))
            .dataFetcher("getAccessToken",
                new GetAccessTokenResolver(tokenService))
            .dataFetcher("container", new AuthenticatedResolver<>(
                new LoadableTypeResolver<>(containerType,
                    (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("listDomains",
                new ListDomainsResolver(this.entityClient))
            .dataFetcher("listSecrets",
                new ListSecretsResolver(this.entityClient))
            .dataFetcher("getSecretValues",
                new GetSecretValuesResolver(this.entityClient, this.secretService))
            .dataFetcher("listIngestionSources",
                new ListIngestionSourcesResolver(this.entityClient))
            .dataFetcher("ingestionSource",
                new GetIngestionSourceResolver(this.entityClient))
            .dataFetcher("executionRequest",
                new GetIngestionExecutionRequestResolver(this.entityClient))
        );
    }

    private void configureMutationResolvers(final RuntimeWiring.Builder builder) {
        builder.type("Mutation", typeWiring -> typeWiring
            .dataFetcher("updateDataset", new AuthenticatedResolver<>(new MutableTypeResolver<>(datasetType)))
            .dataFetcher("updateTag", new AuthenticatedResolver<>(new MutableTypeResolver<>(tagType)))
            .dataFetcher("setTagColor", new SetTagColorResolver(entityClient, entityService))
            .dataFetcher("updateChart", new AuthenticatedResolver<>(new MutableTypeResolver<>(chartType)))
            .dataFetcher("updateDashboard", new AuthenticatedResolver<>(new MutableTypeResolver<>(dashboardType)))
            .dataFetcher("updateNotebook", new AuthenticatedResolver<>(new MutableTypeResolver<>(notebookType)))
            .dataFetcher("updateDataJob", new AuthenticatedResolver<>(new MutableTypeResolver<>(dataJobType)))
            .dataFetcher("updateDataFlow", new AuthenticatedResolver<>(new MutableTypeResolver<>(dataFlowType)))
            .dataFetcher("updateCorpUserProperties", new MutableTypeResolver<>(corpUserType))
            .dataFetcher("updateCorpGroupProperties", new MutableTypeResolver<>(corpGroupType))
            .dataFetcher("addTag", new AuthenticatedResolver<>(new AddTagResolver(entityService)))
            .dataFetcher("removeTag", new AuthenticatedResolver<>(new RemoveTagResolver(entityService)))
            .dataFetcher("addTerm", new AuthenticatedResolver<>(new AddTermResolver(entityService)))
            .dataFetcher("removeTerm", new AuthenticatedResolver<>(new RemoveTermResolver(entityService)))
            .dataFetcher("createPolicy", new UpsertPolicyResolver(this.entityClient))
            .dataFetcher("updatePolicy", new UpsertPolicyResolver(this.entityClient))
            .dataFetcher("deletePolicy", new DeletePolicyResolver(this.entityClient))
            .dataFetcher("updateDescription", new UpdateDescriptionResolver(entityService))
            .dataFetcher("addOwner", new AddOwnerResolver(entityService))
            .dataFetcher("removeOwner", new RemoveOwnerResolver(entityService))
            .dataFetcher("addLink", new AddLinkResolver(entityService))
            .dataFetcher("removeLink", new RemoveLinkResolver(entityService))
            .dataFetcher("addGroupMembers", new AddGroupMembersResolver(this.entityClient))
            .dataFetcher("removeGroupMembers", new RemoveGroupMembersResolver(this.entityClient))
            .dataFetcher("createGroup", new CreateGroupResolver(this.entityClient))
            .dataFetcher("removeUser", new RemoveUserResolver(this.entityClient))
            .dataFetcher("removeGroup", new RemoveGroupResolver(this.entityClient))
            .dataFetcher("updateUserStatus", new UpdateUserStatusResolver(this.entityClient))
            .dataFetcher("createDomain", new CreateDomainResolver(this.entityClient))
            .dataFetcher("setDomain", new SetDomainResolver(this.entityClient, this.entityService))
            .dataFetcher("updateDeprecation", new UpdateDeprecationResolver(this.entityClient, this.entityService))
            .dataFetcher("unsetDomain", new UnsetDomainResolver(this.entityClient, this.entityService))
            .dataFetcher("createSecret", new CreateSecretResolver(this.entityClient, this.secretService))
            .dataFetcher("deleteSecret", new DeleteSecretResolver(this.entityClient))
            .dataFetcher("createIngestionSource", new UpsertIngestionSourceResolver(this.entityClient))
            .dataFetcher("updateIngestionSource", new UpsertIngestionSourceResolver(this.entityClient))
            .dataFetcher("deleteIngestionSource", new DeleteIngestionSourceResolver(this.entityClient))
            .dataFetcher("createIngestionExecutionRequest", new CreateIngestionExecutionRequestResolver(this.entityClient, this.ingestionConfiguration))
            .dataFetcher("cancelIngestionExecutionRequest", new CancelIngestionExecutionRequestResolver(this.entityClient))
            .dataFetcher("deleteAssertion", new DeleteAssertionResolver(this.entityClient, this.entityService))
        );
    }

    private void configureGenericEntityResolvers(final RuntimeWiring.Builder builder) {
        builder
            .type("SearchResult", typeWiring -> typeWiring
                .dataFetcher("entity", new AuthenticatedResolver<>(
                    new EntityTypeResolver(
                        entityTypes.stream().collect(Collectors.toList()),
                        (env) -> ((SearchResult) env.getSource()).getEntity()))
                )
            )
            .type("SearchAcrossLineageResult", typeWiring -> typeWiring
                .dataFetcher("entity", new AuthenticatedResolver<>(
                    new EntityTypeResolver(
                        entityTypes.stream().collect(Collectors.toList()),
                        (env) -> ((SearchAcrossLineageResult) env.getSource()).getEntity()))
                )
            )
            .type("AggregationMetadata", typeWiring -> typeWiring
                .dataFetcher("entity", new EntityTypeResolver(
                    entityTypes.stream().collect(Collectors.toList()),
                    (env) -> ((AggregationMetadata) env.getSource()).getEntity()))
            )
            .type("RecommendationContent", typeWiring -> typeWiring
                .dataFetcher("entity", new EntityTypeResolver(
                    entityTypes.stream().collect(Collectors.toList()),
                    (env) -> ((RecommendationContent) env.getSource()).getEntity()))
            )
            .type("BrowseResults", typeWiring -> typeWiring
                .dataFetcher("entities", new AuthenticatedResolver<>(
                    new EntityTypeBatchResolver(
                        entityTypes.stream().collect(Collectors.toList()),
                        (env) -> ((BrowseResults) env.getSource()).getEntities()))
                )
            )
            .type("EntityRelationshipLegacy", typeWiring -> typeWiring
                .dataFetcher("entity", new AuthenticatedResolver<>(
                    new EntityTypeResolver(
                        new ArrayList<>(entityTypes),
                        (env) -> ((EntityRelationshipLegacy) env.getSource()).getEntity()))
                )
            )
            .type("EntityRelationship", typeWiring -> typeWiring
                .dataFetcher("entity", new AuthenticatedResolver<>(
                    new EntityTypeResolver(
                        new ArrayList<>(entityTypes),
                        (env) -> ((EntityRelationship) env.getSource()).getEntity()))
                )
            )
            .type("LineageRelationship", typeWiring -> typeWiring
                .dataFetcher("entity", new AuthenticatedResolver<>(
                    new EntityTypeResolver(
                        new ArrayList<>(entityTypes),
                        (env) -> ((LineageRelationship) env.getSource()).getEntity()))
                )
            )
            .type("ListDomainsResult", typeWiring -> typeWiring
                .dataFetcher("domains",
                    new LoadableTypeBatchResolver<>(domainType,
                        (env) -> ((ListDomainsResult) env.getSource()).getDomains().stream()
                            .map(Domain::getUrn)
                            .collect(Collectors.toList())))
            )
            .type("AutoCompleteResults", typeWiring -> typeWiring
                .dataFetcher("entities",
                    new EntityTypeBatchResolver(
                        new ArrayList<>(entityTypes),
                        (env) -> ((AutoCompleteResults) env.getSource()).getEntities()))
            )
            .type("AutoCompleteResultForEntity", typeWiring -> typeWiring
                .dataFetcher("entities",
                    new EntityTypeBatchResolver(
                        new ArrayList<>(entityTypes),
                        (env) -> ((AutoCompleteResultForEntity) env.getSource()).getEntities()))
            )
            .type("PolicyMatchCriterionValue", typeWiring -> typeWiring
                .dataFetcher("entity", new AuthenticatedResolver<>(
                    new EntityTypeResolver(
                        new ArrayList<>(entityTypes),
                        (env) -> ((PolicyMatchCriterionValue) env.getSource()).getEntity()))
                )
            );
    }

    /**
     * Configures resolvers responsible for resolving the {@link com.linkedin.datahub.graphql.generated.Dataset} type.
     */
    private void configureDatasetResolvers(final RuntimeWiring.Builder builder) {
        builder
            .type("Dataset", typeWiring -> typeWiring
                .dataFetcher("relationships", new AuthenticatedResolver<>(
                    new EntityRelationshipsResultResolver(graphClient)
                ))
                .dataFetcher("lineage", new AuthenticatedResolver<>(
                    new EntityLineageResultResolver(graphClient)
                ))
                .dataFetcher("domain",
                    new LoadableTypeResolver<>(
                        domainType,
                        (env) -> {
                            final Dataset dataset = env.getSource();
                            return dataset.getDomain() != null ? dataset.getDomain().getUrn() : null;
                        }))
                .dataFetcher("platform", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(dataPlatformType,
                            (env) -> ((Dataset) env.getSource()).getPlatform().getUrn()))
                )
                .dataFetcher("container",
                    new LoadableTypeResolver<>(containerType,
                        (env) -> {
                            final Dataset dataset = env.getSource();
                            return dataset.getContainer() != null ? dataset.getContainer().getUrn() : null;
                        })
                )
                .dataFetcher("datasetProfiles", new AuthenticatedResolver<>(
                    new TimeSeriesAspectResolver(
                        this.entityClient,
                        "dataset",
                        "datasetProfile",
                        DatasetProfileMapper::map
                    )
                ))
                .dataFetcher("operations", new AuthenticatedResolver<>(
                    new TimeSeriesAspectResolver(
                            this.entityClient,
                            "dataset",
                            "operation",
                            OperationMapper::map
                    )
                ))
                .dataFetcher("usageStats", new AuthenticatedResolver<>(new UsageTypeResolver()))
                .dataFetcher("health", new DatasetHealthResolver(graphClient, timeseriesAspectService))
                .dataFetcher("schemaMetadata", new AuthenticatedResolver<>(
                    new AspectResolver())
                )
                .dataFetcher("assertions", new EntityAssertionsResolver(entityClient, graphClient))
               .dataFetcher("aspects", new AuthenticatedResolver<>(
                   new WeaklyTypedAspectsResolver(entityClient, entityRegistry))
               )
               .dataFetcher("subTypes", new AuthenticatedResolver(new SubTypesResolver(
                   this.entityClient,
                   "dataset",
                   "subTypes")))
                .dataFetcher("runs", new EntityRunsResolver(entityClient))
            )
            .type("Owner", typeWiring -> typeWiring
                    .dataFetcher("owner", new AuthenticatedResolver<>(
                            new OwnerTypeResolver<>(ownerTypes,
                                    (env) -> ((Owner) env.getSource()).getOwner()))
                    )
            )
            .type("UserUsageCounts", typeWiring -> typeWiring
                .dataFetcher("user", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(corpUserType,
                        (env) -> ((UserUsageCounts) env.getSource()).getUser().getUrn()))
                )
            )
            .type("ForeignKeyConstraint", typeWiring -> typeWiring
                .dataFetcher("foreignDataset", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(datasetType,
                        (env) -> ((ForeignKeyConstraint) env.getSource()).getForeignDataset().getUrn()))
                )
            )
            .type("InstitutionalMemoryMetadata", typeWiring -> typeWiring
                .dataFetcher("author", new AuthenticatedResolver<>(
                        new LoadableTypeResolver<>(corpUserType,
                                (env) -> ((InstitutionalMemoryMetadata) env.getSource()).getAuthor().getUrn()))
                )
            );

    }

    private void configureGlossaryTermResolvers(final RuntimeWiring.Builder builder) {
        builder.type("GlossaryTerm", typeWiring -> typeWiring
            .dataFetcher("schemaMetadata", new AuthenticatedResolver<>(
                    new AspectResolver())
            )
        );
    }

    /**
     * Configures resolvers responsible for resolving the {@link com.linkedin.datahub.graphql.generated.CorpUser} type.
     */
    private void configureCorpUserResolvers(final RuntimeWiring.Builder builder) {
        builder.type("CorpUser", typeWiring -> typeWiring
            .dataFetcher("relationships", new AuthenticatedResolver<>(
                new EntityRelationshipsResultResolver(graphClient)
            ))
        );
        builder.type("CorpUserInfo", typeWiring -> typeWiring
            .dataFetcher("manager", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(corpUserType,
                            (env) -> ((CorpUserInfo) env.getSource()).getManager().getUrn()))
            )
        );
    }

    /**
     * Configures resolvers responsible for resolving the {@link com.linkedin.datahub.graphql.generated.CorpGroup} type.
     */
    private void configureCorpGroupResolvers(final RuntimeWiring.Builder builder) {
        builder.type("CorpGroup", typeWiring -> typeWiring
            .dataFetcher("relationships", new AuthenticatedResolver<>(
                new EntityRelationshipsResultResolver(graphClient)
            ))
        );
        builder.type("CorpGroupInfo", typeWiring -> typeWiring
            .dataFetcher("admins", new AuthenticatedResolver<>(
                    new LoadableTypeBatchResolver<>(corpUserType,
                            (env) -> ((CorpGroupInfo) env.getSource()).getAdmins().stream()
                                    .map(CorpUser::getUrn)
                                    .collect(Collectors.toList())))
            )
            .dataFetcher("members", new AuthenticatedResolver<>(
                    new LoadableTypeBatchResolver<>(corpUserType,
                            (env) -> ((CorpGroupInfo) env.getSource()).getMembers().stream()
                                    .map(CorpUser::getUrn)
                                    .collect(Collectors.toList())))
            )
        );
    }

    private void configureTagAssociationResolver(final RuntimeWiring.Builder builder) {
        builder.type("Tag", typeWiring -> typeWiring
            .dataFetcher("relationships", new AuthenticatedResolver<>(
                new EntityRelationshipsResultResolver(graphClient)
            ))
        );
        builder.type("TagAssociation", typeWiring -> typeWiring
            .dataFetcher("tag", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(tagType,
                            (env) -> ((com.linkedin.datahub.graphql.generated.TagAssociation) env.getSource()).getTag().getUrn()))
            )
        );
    }

  /**
   * Configures resolvers responsible for resolving the {@link com.linkedin.datahub.graphql.generated.Notebook} type.
   */
  private void configureNotebookResolvers(final RuntimeWiring.Builder builder) {
    builder.type("Notebook", typeWiring -> typeWiring
        .dataFetcher("relationships", new AuthenticatedResolver<>(
            new EntityRelationshipsResultResolver(graphClient)
        ))
        .dataFetcher("platform", new AuthenticatedResolver<>(
                new LoadableTypeResolver<>(dataPlatformType,
                        (env) -> ((Notebook) env.getSource()).getPlatform().getUrn()))
        )
        .dataFetcher("domain",
            new LoadableTypeResolver<>(domainType,
                (env) -> ((Notebook) env.getSource()).getDomain().getUrn())
        )
    );
  }

    /**
     * Configures resolvers responsible for resolving the {@link com.linkedin.datahub.graphql.generated.Dashboard} type.
     */
    private void configureDashboardResolvers(final RuntimeWiring.Builder builder) {
        builder.type("Dashboard", typeWiring -> typeWiring
            .dataFetcher("relationships", new AuthenticatedResolver<>(
                new EntityRelationshipsResultResolver(graphClient)
            ))
            .dataFetcher("lineage", new AuthenticatedResolver<>(
                new EntityLineageResultResolver(graphClient)
            ))
            .dataFetcher("platform", new AuthenticatedResolver<>(
                new LoadableTypeResolver<>(dataPlatformType,
                    (env) -> ((Dashboard) env.getSource()).getPlatform().getUrn()))
                )
            .dataFetcher("domain",
                new LoadableTypeResolver<>(
                    domainType,
                    (env) -> {
                        final Dashboard dashboard = env.getSource();
                        return dashboard.getDomain() != null ? dashboard.getDomain().getUrn() : null;
                    }
                )
            )
            .dataFetcher("container",
                new LoadableTypeResolver<>(containerType,
                    (env) -> {
                        final Dashboard dashboard = env.getSource();
                        return dashboard.getContainer() != null ? dashboard.getContainer().getUrn() : null;
                    })
            )
        );
        builder.type("DashboardInfo", typeWiring -> typeWiring
            .dataFetcher("charts", new AuthenticatedResolver<>(
                new LoadableTypeBatchResolver<>(chartType,
                    (env) -> ((DashboardInfo) env.getSource()).getCharts().stream()
                        .map(Chart::getUrn)
                        .collect(Collectors.toList())))
            )
        );
    }

    /**
     * Configures resolvers responsible for resolving the {@link com.linkedin.datahub.graphql.generated.Chart} type.
     */
    private void configureChartResolvers(final RuntimeWiring.Builder builder) {
        builder.type("Chart", typeWiring -> typeWiring
            .dataFetcher("relationships", new AuthenticatedResolver<>(
                new EntityRelationshipsResultResolver(graphClient)
            ))
            .dataFetcher("lineage", new AuthenticatedResolver<>(
                new EntityLineageResultResolver(graphClient)
            ))
            .dataFetcher("platform", new AuthenticatedResolver<>(
                new LoadableTypeResolver<>(dataPlatformType,
                    (env) -> ((Chart) env.getSource()).getPlatform().getUrn()))
            )
            .dataFetcher("domain",
                new LoadableTypeResolver<>(
                    domainType,
                    (env) -> {
                        final Chart chart = env.getSource();
                        return chart.getDomain() != null ? chart.getDomain().getUrn() : null;
                    })
            )
            .dataFetcher("container",
                new LoadableTypeResolver<>(
                    containerType,
                    (env) -> {
                        final Chart chart = env.getSource();
                        return chart.getContainer() != null ? chart.getContainer().getUrn() : null;
                    })
            )
        );
        builder.type("ChartInfo", typeWiring -> typeWiring
            .dataFetcher("inputs", new AuthenticatedResolver<>(
                    new LoadableTypeBatchResolver<>(datasetType,
                            (env) -> ((ChartInfo) env.getSource()).getInputs().stream()
                                    .map(Dataset::getUrn)
                                    .collect(Collectors.toList())))
            )
        );
    }

    /**
     * Configures {@link graphql.schema.TypeResolver}s for any GQL 'union' or 'interface' types.
     */
    private void configureTypeResolvers(final RuntimeWiring.Builder builder) {
        builder
            .type("Entity", typeWiring -> typeWiring
                .typeResolver(new EntityInterfaceTypeResolver(loadableTypes.stream()
                        .filter(graphType -> graphType instanceof EntityType)
                        .map(graphType -> (EntityType<?>) graphType)
                        .collect(Collectors.toList())
                )))
            .type("EntityWithRelationships", typeWiring -> typeWiring
                .typeResolver(new EntityInterfaceTypeResolver(loadableTypes.stream()
                        .filter(graphType -> graphType instanceof EntityType)
                        .map(graphType -> (EntityType<?>) graphType)
                        .collect(Collectors.toList())
                )))
            .type("OwnerType", typeWiring -> typeWiring
                .typeResolver(new EntityInterfaceTypeResolver(ownerTypes.stream()
                    .filter(graphType -> graphType instanceof EntityType)
                    .map(graphType -> (EntityType<?>) graphType)
                    .collect(Collectors.toList())
                )))
            .type("PlatformSchema", typeWiring -> typeWiring
                    .typeResolver(new PlatformSchemaUnionTypeResolver())
            )
            .type("HyperParameterValueType", typeWiring -> typeWiring
                    .typeResolver(new HyperParameterValueTypeResolver())
            )
            .type("Aspect", typeWiring -> typeWiring.typeResolver(new AspectInterfaceTypeResolver()))
            .type("TimeSeriesAspect", typeWiring -> typeWiring.typeResolver(new TimeSeriesAspectInterfaceTypeResolver()))
            .type("ResultsType", typeWiring -> typeWiring
                    .typeResolver(new ResultsTypeResolver()));
    }

    /**
     * Configures custom type extensions leveraged within our GraphQL schema.
     */
    private void configureTypeExtensions(final RuntimeWiring.Builder builder) {
        builder.scalar(GraphQLLong);
    }

    /**
     * Configures resolvers responsible for resolving the {@link com.linkedin.datahub.graphql.generated.DataJob} type.
     */
    private void configureDataJobResolvers(final RuntimeWiring.Builder builder) {
        builder
            .type("DataJob", typeWiring -> typeWiring
                .dataFetcher("relationships", new AuthenticatedResolver<>(
                    new EntityRelationshipsResultResolver(graphClient)
                ))
                .dataFetcher("lineage", new AuthenticatedResolver<>(
                    new EntityLineageResultResolver(graphClient)
                ))
                .dataFetcher("dataFlow", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(dataFlowType,
                        (env) -> ((DataJob) env.getSource()).getDataFlow().getUrn()))
                )
                .dataFetcher("domain",
                    new LoadableTypeResolver<>(
                        domainType,
                        (env) -> {
                            final DataJob dataJob = env.getSource();
                            return dataJob.getDomain() != null ? dataJob.getDomain().getUrn() : null;
                        })
                )
                .dataFetcher("runs", new DataJobRunsResolver(entityClient))
            )
            .type("DataJobInputOutput", typeWiring -> typeWiring
                .dataFetcher("inputDatasets", new AuthenticatedResolver<>(
                    new LoadableTypeBatchResolver<>(datasetType,
                        (env) -> ((DataJobInputOutput) env.getSource()).getInputDatasets().stream()
                                .map(Dataset::getUrn)
                                .collect(Collectors.toList())))
                )
                .dataFetcher("outputDatasets", new AuthenticatedResolver<>(
                    new LoadableTypeBatchResolver<>(datasetType,
                        (env) -> ((DataJobInputOutput) env.getSource()).getOutputDatasets().stream()
                                .map(Dataset::getUrn)
                                .collect(Collectors.toList())))
                )
                .dataFetcher("inputDatajobs", new AuthenticatedResolver<>(
                    new LoadableTypeBatchResolver<>(dataJobType,
                        (env) -> ((DataJobInputOutput) env.getSource()).getInputDatajobs().stream()
                            .map(DataJob::getUrn)
                            .collect(Collectors.toList())))
                )
            );
    }

    /**
     * Configures resolvers responsible for resolving the {@link com.linkedin.datahub.graphql.generated.DataFlow} type.
     */
    private void configureDataFlowResolvers(final RuntimeWiring.Builder builder) {
        builder
            .type("DataFlow", typeWiring -> typeWiring
                .dataFetcher("relationships", new AuthenticatedResolver<>(
                    new EntityRelationshipsResultResolver(graphClient)
                ))
                .dataFetcher("lineage", new AuthenticatedResolver<>(
                    new EntityLineageResultResolver(graphClient)
                ))
                .dataFetcher("platform", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(dataPlatformType,
                        (env) -> ((DataFlow) env.getSource()).getPlatform().getUrn()))
                )
                .dataFetcher("domain",
                    new LoadableTypeResolver<>(
                        domainType,
                        (env) -> {
                            final DataFlow dataFlow = env.getSource();
                            return dataFlow.getDomain() != null ? dataFlow.getDomain().getUrn() : null;
                        })
                )
            );
    }

    /**
     * Configures resolvers responsible for resolving the {@link com.linkedin.datahub.graphql.generated.MLFeatureTable} type.
     */
    private void configureMLFeatureTableResolvers(final RuntimeWiring.Builder builder) {
        builder
            .type("MLFeatureTable", typeWiring -> typeWiring
                .dataFetcher("relationships", new AuthenticatedResolver<>(
                    new EntityRelationshipsResultResolver(graphClient)
                ))
                .dataFetcher("lineage", new AuthenticatedResolver<>(
                    new EntityLineageResultResolver(graphClient)
                ))
                .dataFetcher("platform", new AuthenticatedResolver<>(
                        new LoadableTypeResolver<>(dataPlatformType,
                                (env) -> ((MLFeatureTable) env.getSource()).getPlatform().getUrn()))
                )
                .dataFetcher("domain",
                    new LoadableTypeResolver<>(
                        domainType,
                        (env) -> {
                            final MLFeatureTable entity = env.getSource();
                            return entity.getDomain() != null ? entity.getDomain().getUrn() : null;
                        }))
            )
            .type("MLFeatureTableProperties", typeWiring -> typeWiring
                .dataFetcher("mlFeatures", new AuthenticatedResolver<>(
                                new LoadableTypeBatchResolver<>(mlFeatureType,
                                        (env) ->
                                            ((MLFeatureTableProperties) env.getSource()).getMlFeatures() != null
                                                ? ((MLFeatureTableProperties) env.getSource()).getMlFeatures().stream()
                                        .map(MLFeature::getUrn)
                                        .collect(Collectors.toList()) : ImmutableList.of()))
                )
                .dataFetcher("mlPrimaryKeys", new AuthenticatedResolver<>(
                                new LoadableTypeBatchResolver<>(mlPrimaryKeyType,
                                        (env) ->
                                            ((MLFeatureTableProperties) env.getSource()).getMlPrimaryKeys() != null
                                                ? ((MLFeatureTableProperties) env.getSource()).getMlPrimaryKeys().stream()
                                        .map(MLPrimaryKey::getUrn)
                                        .collect(Collectors.toList()) : ImmutableList.of()))
                )
            )
            .type("MLFeatureProperties", typeWiring -> typeWiring
                .dataFetcher("sources", new AuthenticatedResolver<>(
                        new LoadableTypeBatchResolver<>(datasetType,
                                (env) -> ((MLFeatureProperties) env.getSource()).getSources().stream()
                                        .map(Dataset::getUrn)
                                        .collect(Collectors.toList())))
                )
            )
            .type("MLPrimaryKeyProperties", typeWiring -> typeWiring
                .dataFetcher("sources", new AuthenticatedResolver<>(
                        new LoadableTypeBatchResolver<>(datasetType,
                                (env) -> ((MLPrimaryKeyProperties) env.getSource()).getSources().stream()
                                        .map(Dataset::getUrn)
                                        .collect(Collectors.toList())))
                )
            )
            .type("MLModel", typeWiring -> typeWiring
                .dataFetcher("relationships", new AuthenticatedResolver<>(
                    new EntityRelationshipsResultResolver(graphClient)
                ))
                .dataFetcher("lineage", new AuthenticatedResolver<>(
                    new EntityLineageResultResolver(graphClient)
                ))
                .dataFetcher("platform", new AuthenticatedResolver<>(
                        new LoadableTypeResolver<>(dataPlatformType,
                                (env) -> ((MLModel) env.getSource()).getPlatform().getUrn()))
                )
                .dataFetcher("domain",
                    new LoadableTypeResolver<>(
                        domainType,
                        (env) -> {
                            final MLModel mlModel = env.getSource();
                            return mlModel.getDomain() != null ? mlModel.getDomain().getUrn() : null;
                        }))
            )
            .type("MLModelProperties", typeWiring -> typeWiring
                .dataFetcher("groups", new AuthenticatedResolver<>(
                    new LoadableTypeBatchResolver<>(mlModelGroupType,
                        (env) -> {
                            MLModelProperties properties = env.getSource();
                            if (properties.getGroups() != null) {
                                return properties.getGroups().stream()
                                    .map(MLModelGroup::getUrn)
                                    .collect(Collectors.toList());
                            }
                            return Collections.emptyList();
                        }))
                )
            )
            .type("MLModelGroup", typeWiring -> typeWiring
                .dataFetcher("relationships", new AuthenticatedResolver<>(
                    new EntityRelationshipsResultResolver(graphClient)
                ))
                .dataFetcher("lineage", new AuthenticatedResolver<>(
                    new EntityLineageResultResolver(graphClient)
                ))
                .dataFetcher("platform", new AuthenticatedResolver<>(
                        new LoadableTypeResolver<>(dataPlatformType,
                                (env) -> ((MLModelGroup) env.getSource()).getPlatform().getUrn()))
                )
                .dataFetcher("domain",
                    new LoadableTypeResolver<>(
                        domainType,
                        (env) -> {
                            final MLModelGroup entity = env.getSource();
                            return entity.getDomain() != null ? entity.getDomain().getUrn() : null;
                        }))
            )
            .type("MLFeature", typeWiring -> typeWiring
                .dataFetcher("relationships", new AuthenticatedResolver<>(
                    new EntityRelationshipsResultResolver(graphClient)
                ))
                .dataFetcher("lineage", new AuthenticatedResolver<>(
                    new EntityLineageResultResolver(graphClient)
                ))
                .dataFetcher("domain",
                    new LoadableTypeResolver<>(
                        domainType,
                        (env) -> {
                            final MLFeature entity = env.getSource();
                            return entity.getDomain() != null ? entity.getDomain().getUrn() : null;
                        }))
            )
            .type("MLPrimaryKey", typeWiring -> typeWiring
                .dataFetcher("relationships", new AuthenticatedResolver<>(
                    new EntityRelationshipsResultResolver(graphClient)
                ))
                .dataFetcher("lineage", new AuthenticatedResolver<>(
                    new EntityLineageResultResolver(graphClient)
                ))
                .dataFetcher("domain",
                    new LoadableTypeResolver<>(
                        domainType,
                        (env) -> {
                            final MLPrimaryKey entity = env.getSource();
                            return entity.getDomain() != null ? entity.getDomain().getUrn() : null;
                        }))
            );
    }

    private void configureGlossaryRelationshipResolvers(final RuntimeWiring.Builder builder) {
        builder.type("GlossaryTerm", typeWiring -> typeWiring.dataFetcher("relationships",
            new AuthenticatedResolver<>(new EntityRelationshipsResultResolver(graphClient))));
    }

    private void configureDomainResolvers(final RuntimeWiring.Builder builder) {
        builder.type("Domain", typeWiring -> typeWiring
            .dataFetcher("entities", new DomainEntitiesResolver(this.entityClient))
            .dataFetcher("relationships", new AuthenticatedResolver<>(
                new EntityRelationshipsResultResolver(graphClient)
            ))
        );
    }

    private void configureAssertionResolvers(final RuntimeWiring.Builder builder) {
        builder.type("Assertion", typeWiring -> typeWiring.dataFetcher("relationships",
            new AuthenticatedResolver<>(new EntityRelationshipsResultResolver(graphClient)))
            .dataFetcher("platform", new AuthenticatedResolver<>(
                new LoadableTypeResolver<>(dataPlatformType, (env) -> ((Assertion) env.getSource()).getPlatform().getUrn())))
            .dataFetcher("runEvents", new AssertionRunEventResolver(entityClient)));
    }

    private void configurePolicyResolvers(final RuntimeWiring.Builder builder) {
        // Register resolvers for "resolvedUsers" and "resolvedGroups" field of the Policy type.
        builder.type("ActorFilter", typeWiring -> typeWiring
            .dataFetcher("resolvedUsers", new LoadableTypeBatchResolver<>(corpUserType,
                (env) -> {
                    final ActorFilter filter = env.getSource();
                    return filter.getUsers();
                }
            ))
            .dataFetcher("resolvedGroups", new LoadableTypeBatchResolver<>(corpGroupType,
                    (env) -> {
                        final ActorFilter filter = env.getSource();
                        return filter.getGroups();
                    }
            ))
        );
    }

    private void configureDataProcessInstanceResolvers(final RuntimeWiring.Builder builder) {
        builder.type("DataProcessInstance", typeWiring -> typeWiring
            .dataFetcher("relationships",
                new AuthenticatedResolver<>(new EntityRelationshipsResultResolver(graphClient)))
            .dataFetcher("lineage", new AuthenticatedResolver<>(
                new EntityLineageResultResolver(graphClient)
            ))
            .dataFetcher("state", new AuthenticatedResolver<>(
                new TimeSeriesAspectResolver(
                    this.entityClient,
                    "dataProcessInstance",
                    DATA_PROCESS_INSTANCE_RUN_EVENT_ASPECT_NAME,
                    DataProcessInstanceRunEventMapper::map
                )
            ))
        );
    }

    private <T> DataLoader<String, DataFetcherResult<T>> createDataLoader(final LoadableType<T> graphType, final QueryContext queryContext) {
        BatchLoaderContextProvider contextProvider = () -> queryContext;
        DataLoaderOptions loaderOptions = DataLoaderOptions.newOptions().setBatchLoaderContextProvider(contextProvider);
        return DataLoader.newDataLoader((keys, context) -> CompletableFuture.supplyAsync(() -> {
            try {
                log.debug(String.format("Batch loading entities of type: %s, keys: %s", graphType.name(), keys));
                return graphType.batchLoad(keys, context.getContext());
            } catch (Exception e) {
                log.error(String.format("Failed to load Entities of type: %s, keys: %s", graphType.name(), keys) + " " + e.getMessage());
                throw new RuntimeException(String.format("Failed to retrieve entities of type %s", graphType.name()), e);
            }
        }), loaderOptions);
    }

    private void configureIngestionSourceResolvers(final RuntimeWiring.Builder builder) {
        builder.type("IngestionSource", typeWiring -> typeWiring.dataFetcher("executions", new IngestionSourceExecutionRequestsResolver(entityClient)));
    }

    private DataLoader<VersionedAspectKey, DataFetcherResult<Aspect>> createAspectLoader(final QueryContext queryContext) {
        BatchLoaderContextProvider contextProvider = () -> queryContext;
        DataLoaderOptions loaderOptions = DataLoaderOptions.newOptions().setBatchLoaderContextProvider(contextProvider);
        return DataLoader.newDataLoader((keys, context) -> CompletableFuture.supplyAsync(() -> {
            try {
                log.debug(String.format("Batch loading aspects with keys: %s", keys));
                return aspectType.batchLoad(keys, context.getContext());
            } catch (Exception e) {
                log.error(String.format("Failed to load Aspect for entity. keys: %s", keys) + " " + e.getMessage());
                throw new RuntimeException(String.format("Failed to retrieve entities of type Aspect", e));
            }
        }), loaderOptions);
    }

    private DataLoader<UsageStatsKey, DataFetcherResult<UsageQueryResult>> createUsageLoader(final QueryContext queryContext) {
        BatchLoaderContextProvider contextProvider = () -> queryContext;
        DataLoaderOptions loaderOptions = DataLoaderOptions.newOptions().setBatchLoaderContextProvider(contextProvider);
        return DataLoader.newDataLoader((keys, context) -> CompletableFuture.supplyAsync(() -> {
            try {
                return usageType.batchLoad(keys, context.getContext());
            } catch (Exception e) {
                throw new RuntimeException(String.format("Failed to retrieve usage stats", e));
            }
        }), loaderOptions);
    }
}
