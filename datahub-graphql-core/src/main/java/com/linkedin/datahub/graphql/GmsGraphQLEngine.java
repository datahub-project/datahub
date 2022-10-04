package com.linkedin.datahub.graphql;

import com.datahub.authentication.AuthenticationConfiguration;
import com.datahub.authentication.group.GroupService;
import com.datahub.authentication.invite.InviteTokenService;
import com.datahub.authentication.token.StatefulTokenService;
import com.datahub.authentication.user.NativeUserService;
import com.datahub.authorization.AuthorizationConfiguration;
import com.datahub.authorization.role.RoleService;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.VersionedUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.analytics.resolver.AnalyticsChartTypeResolver;
import com.linkedin.datahub.graphql.analytics.resolver.GetChartsResolver;
import com.linkedin.datahub.graphql.analytics.resolver.GetHighlightsResolver;
import com.linkedin.datahub.graphql.analytics.resolver.GetMetadataAnalyticsResolver;
import com.linkedin.datahub.graphql.analytics.resolver.IsAnalyticsEnabledResolver;
import com.linkedin.datahub.graphql.analytics.service.AnalyticsService;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.AccessToken;
import com.linkedin.datahub.graphql.generated.AccessTokenMetadata;
import com.linkedin.datahub.graphql.generated.ActorFilter;
import com.linkedin.datahub.graphql.generated.AggregationMetadata;
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
import com.linkedin.datahub.graphql.generated.DashboardStatsSummary;
import com.linkedin.datahub.graphql.generated.DashboardUserUsageCounts;
import com.linkedin.datahub.graphql.generated.DataFlow;
import com.linkedin.datahub.graphql.generated.DataJob;
import com.linkedin.datahub.graphql.generated.DataJobInputOutput;
import com.linkedin.datahub.graphql.generated.DataPlatformInstance;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.DatasetStatsSummary;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.EntityRelationship;
import com.linkedin.datahub.graphql.generated.EntityRelationshipLegacy;
import com.linkedin.datahub.graphql.generated.ForeignKeyConstraint;
import com.linkedin.datahub.graphql.generated.GetRootGlossaryNodesResult;
import com.linkedin.datahub.graphql.generated.GetRootGlossaryTermsResult;
import com.linkedin.datahub.graphql.generated.GlossaryNode;
import com.linkedin.datahub.graphql.generated.GlossaryTerm;
import com.linkedin.datahub.graphql.generated.GlossaryTermAssociation;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.datahub.graphql.generated.InstitutionalMemoryMetadata;
import com.linkedin.datahub.graphql.generated.LineageRelationship;
import com.linkedin.datahub.graphql.generated.ListAccessTokenResult;
import com.linkedin.datahub.graphql.generated.ListDomainsResult;
import com.linkedin.datahub.graphql.generated.ListTestsResult;
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
import com.linkedin.datahub.graphql.generated.SiblingProperties;
import com.linkedin.datahub.graphql.generated.Test;
import com.linkedin.datahub.graphql.generated.TestResult;
import com.linkedin.datahub.graphql.generated.UserUsageCounts;
import com.linkedin.datahub.graphql.resolvers.MeResolver;
import com.linkedin.datahub.graphql.resolvers.assertion.AssertionRunEventResolver;
import com.linkedin.datahub.graphql.resolvers.assertion.DeleteAssertionResolver;
import com.linkedin.datahub.graphql.resolvers.assertion.EntityAssertionsResolver;
import com.linkedin.datahub.graphql.resolvers.auth.CreateAccessTokenResolver;
import com.linkedin.datahub.graphql.resolvers.auth.GetAccessTokenResolver;
import com.linkedin.datahub.graphql.resolvers.auth.ListAccessTokensResolver;
import com.linkedin.datahub.graphql.resolvers.auth.RevokeAccessTokenResolver;
import com.linkedin.datahub.graphql.resolvers.browse.BrowsePathsResolver;
import com.linkedin.datahub.graphql.resolvers.browse.BrowseResolver;
import com.linkedin.datahub.graphql.resolvers.browse.EntityBrowsePathsResolver;
import com.linkedin.datahub.graphql.resolvers.chart.ChartStatsSummaryResolver;
import com.linkedin.datahub.graphql.resolvers.config.AppConfigResolver;
import com.linkedin.datahub.graphql.resolvers.container.ContainerEntitiesResolver;
import com.linkedin.datahub.graphql.resolvers.container.ParentContainersResolver;
import com.linkedin.datahub.graphql.resolvers.dashboard.DashboardStatsSummaryResolver;
import com.linkedin.datahub.graphql.resolvers.dashboard.DashboardUsageStatsResolver;
import com.linkedin.datahub.graphql.resolvers.dataset.DatasetHealthResolver;
import com.linkedin.datahub.graphql.resolvers.dataset.DatasetStatsSummaryResolver;
import com.linkedin.datahub.graphql.resolvers.dataset.DatasetUsageStatsResolver;
import com.linkedin.datahub.graphql.resolvers.deprecation.UpdateDeprecationResolver;
import com.linkedin.datahub.graphql.resolvers.domain.CreateDomainResolver;
import com.linkedin.datahub.graphql.resolvers.domain.DeleteDomainResolver;
import com.linkedin.datahub.graphql.resolvers.domain.DomainEntitiesResolver;
import com.linkedin.datahub.graphql.resolvers.domain.ListDomainsResolver;
import com.linkedin.datahub.graphql.resolvers.domain.SetDomainResolver;
import com.linkedin.datahub.graphql.resolvers.domain.UnsetDomainResolver;
import com.linkedin.datahub.graphql.resolvers.entity.EntityExistsResolver;
import com.linkedin.datahub.graphql.resolvers.glossary.AddRelatedTermsResolver;
import com.linkedin.datahub.graphql.resolvers.glossary.CreateGlossaryNodeResolver;
import com.linkedin.datahub.graphql.resolvers.glossary.CreateGlossaryTermResolver;
import com.linkedin.datahub.graphql.resolvers.glossary.DeleteGlossaryEntityResolver;
import com.linkedin.datahub.graphql.resolvers.glossary.GetRootGlossaryNodesResolver;
import com.linkedin.datahub.graphql.resolvers.glossary.GetRootGlossaryTermsResolver;
import com.linkedin.datahub.graphql.resolvers.glossary.ParentNodesResolver;
import com.linkedin.datahub.graphql.resolvers.glossary.RemoveRelatedTermsResolver;
import com.linkedin.datahub.graphql.resolvers.group.AddGroupMembersResolver;
import com.linkedin.datahub.graphql.resolvers.group.CreateGroupResolver;
import com.linkedin.datahub.graphql.resolvers.group.EntityCountsResolver;
import com.linkedin.datahub.graphql.resolvers.group.ListGroupsResolver;
import com.linkedin.datahub.graphql.resolvers.group.RemoveGroupMembersResolver;
import com.linkedin.datahub.graphql.resolvers.group.RemoveGroupResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.execution.CancelIngestionExecutionRequestResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.execution.CreateIngestionExecutionRequestResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.execution.CreateTestConnectionRequestResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.execution.GetIngestionExecutionRequestResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.execution.IngestionSourceExecutionRequestsResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.execution.RollbackIngestionResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.secret.CreateSecretResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.secret.DeleteSecretResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.secret.GetSecretValuesResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.secret.ListSecretsResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.source.DeleteIngestionSourceResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.source.GetIngestionSourceResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.source.ListIngestionSourcesResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.source.UpsertIngestionSourceResolver;
import com.linkedin.datahub.graphql.resolvers.jobs.DataJobRunsResolver;
import com.linkedin.datahub.graphql.resolvers.jobs.EntityRunsResolver;
import com.linkedin.datahub.graphql.resolvers.load.AspectResolver;
import com.linkedin.datahub.graphql.resolvers.load.BatchGetEntitiesResolver;
import com.linkedin.datahub.graphql.resolvers.load.EntityLineageResultResolver;
import com.linkedin.datahub.graphql.resolvers.load.EntityRelationshipsResultResolver;
import com.linkedin.datahub.graphql.resolvers.load.EntityTypeBatchResolver;
import com.linkedin.datahub.graphql.resolvers.load.EntityTypeResolver;
import com.linkedin.datahub.graphql.resolvers.load.LoadableTypeBatchResolver;
import com.linkedin.datahub.graphql.resolvers.load.LoadableTypeResolver;
import com.linkedin.datahub.graphql.resolvers.load.OwnerTypeResolver;
import com.linkedin.datahub.graphql.resolvers.load.TimeSeriesAspectResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.AddLinkResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.AddOwnerResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.AddOwnersResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.AddTagResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.AddTagsResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.AddTermResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.AddTermsResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.BatchAddOwnersResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.BatchAddTagsResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.BatchAddTermsResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.BatchRemoveOwnersResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.BatchRemoveTagsResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.BatchRemoveTermsResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.BatchSetDomainResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.BatchUpdateDeprecationResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.BatchUpdateSoftDeletedResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.MutableTypeBatchResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.MutableTypeResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.RemoveLinkResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.RemoveOwnerResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.RemoveTagResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.RemoveTermResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.UpdateDescriptionResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.UpdateNameResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.UpdateParentNodeResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.UpdateUserSettingResolver;
import com.linkedin.datahub.graphql.resolvers.operation.ReportOperationResolver;
import com.linkedin.datahub.graphql.resolvers.policy.DeletePolicyResolver;
import com.linkedin.datahub.graphql.resolvers.policy.GetGrantedPrivilegesResolver;
import com.linkedin.datahub.graphql.resolvers.policy.ListPoliciesResolver;
import com.linkedin.datahub.graphql.resolvers.policy.UpsertPolicyResolver;
import com.linkedin.datahub.graphql.resolvers.recommendation.ListRecommendationsResolver;
import com.linkedin.datahub.graphql.resolvers.role.AcceptRoleResolver;
import com.linkedin.datahub.graphql.resolvers.role.BatchAssignRoleResolver;
import com.linkedin.datahub.graphql.resolvers.role.CreateInviteTokenResolver;
import com.linkedin.datahub.graphql.resolvers.role.GetInviteTokenResolver;
import com.linkedin.datahub.graphql.resolvers.role.ListRolesResolver;
import com.linkedin.datahub.graphql.resolvers.search.AutoCompleteForMultipleResolver;
import com.linkedin.datahub.graphql.resolvers.search.AutoCompleteResolver;
import com.linkedin.datahub.graphql.resolvers.search.SearchAcrossEntitiesResolver;
import com.linkedin.datahub.graphql.resolvers.search.SearchAcrossLineageResolver;
import com.linkedin.datahub.graphql.resolvers.search.SearchResolver;
import com.linkedin.datahub.graphql.resolvers.tag.CreateTagResolver;
import com.linkedin.datahub.graphql.resolvers.tag.DeleteTagResolver;
import com.linkedin.datahub.graphql.resolvers.tag.SetTagColorResolver;
import com.linkedin.datahub.graphql.resolvers.test.CreateTestResolver;
import com.linkedin.datahub.graphql.resolvers.test.DeleteTestResolver;
import com.linkedin.datahub.graphql.resolvers.test.ListTestsResolver;
import com.linkedin.datahub.graphql.resolvers.test.TestResultsResolver;
import com.linkedin.datahub.graphql.resolvers.test.UpdateTestResolver;
import com.linkedin.datahub.graphql.resolvers.timeline.GetSchemaBlameResolver;
import com.linkedin.datahub.graphql.resolvers.timeline.GetSchemaVersionListResolver;
import com.linkedin.datahub.graphql.resolvers.type.AspectInterfaceTypeResolver;
import com.linkedin.datahub.graphql.resolvers.type.EntityInterfaceTypeResolver;
import com.linkedin.datahub.graphql.resolvers.type.HyperParameterValueTypeResolver;
import com.linkedin.datahub.graphql.resolvers.type.PlatformSchemaUnionTypeResolver;
import com.linkedin.datahub.graphql.resolvers.type.ResultsTypeResolver;
import com.linkedin.datahub.graphql.resolvers.type.TimeSeriesAspectInterfaceTypeResolver;
import com.linkedin.datahub.graphql.resolvers.user.CreateNativeUserResetTokenResolver;
import com.linkedin.datahub.graphql.resolvers.user.ListUsersResolver;
import com.linkedin.datahub.graphql.resolvers.user.RemoveUserResolver;
import com.linkedin.datahub.graphql.resolvers.user.UpdateUserStatusResolver;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.EntityType;
import com.linkedin.datahub.graphql.types.LoadableType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.aspect.AspectType;
import com.linkedin.datahub.graphql.types.assertion.AssertionType;
import com.linkedin.datahub.graphql.types.auth.AccessTokenMetadataType;
import com.linkedin.datahub.graphql.types.chart.ChartType;
import com.linkedin.datahub.graphql.types.common.mappers.OperationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.types.container.ContainerType;
import com.linkedin.datahub.graphql.types.corpgroup.CorpGroupType;
import com.linkedin.datahub.graphql.types.corpuser.CorpUserType;
import com.linkedin.datahub.graphql.types.dashboard.DashboardType;
import com.linkedin.datahub.graphql.types.dataflow.DataFlowType;
import com.linkedin.datahub.graphql.types.datajob.DataJobType;
import com.linkedin.datahub.graphql.types.dataplatform.DataPlatformType;
import com.linkedin.datahub.graphql.types.dataplatforminstance.DataPlatformInstanceType;
import com.linkedin.datahub.graphql.types.dataprocessinst.mappers.DataProcessInstanceRunEventMapper;
import com.linkedin.datahub.graphql.types.dataset.DatasetType;
import com.linkedin.datahub.graphql.types.dataset.VersionedDatasetType;
import com.linkedin.datahub.graphql.types.dataset.mappers.DatasetProfileMapper;
import com.linkedin.datahub.graphql.types.domain.DomainType;
import com.linkedin.datahub.graphql.types.glossary.GlossaryNodeType;
import com.linkedin.datahub.graphql.types.glossary.GlossaryTermType;
import com.linkedin.datahub.graphql.types.mlmodel.MLFeatureTableType;
import com.linkedin.datahub.graphql.types.mlmodel.MLFeatureType;
import com.linkedin.datahub.graphql.types.mlmodel.MLModelGroupType;
import com.linkedin.datahub.graphql.types.mlmodel.MLModelType;
import com.linkedin.datahub.graphql.types.mlmodel.MLPrimaryKeyType;
import com.linkedin.datahub.graphql.types.notebook.NotebookType;
import com.linkedin.datahub.graphql.types.policy.DataHubPolicyType;
import com.linkedin.datahub.graphql.types.role.DataHubRoleType;
import com.linkedin.datahub.graphql.types.tag.TagType;
import com.linkedin.datahub.graphql.types.test.TestType;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.config.DatahubConfiguration;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.metadata.config.TestsConfiguration;
import com.linkedin.metadata.config.VisualConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.graph.SiblingGraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.recommendation.RecommendationsService;
import com.linkedin.metadata.secret.SecretService;
import com.linkedin.metadata.telemetry.TelemetryConfiguration;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.usage.UsageClient;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.StaticDataFetcher;
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

import static com.linkedin.datahub.graphql.Constants.*;
import static com.linkedin.metadata.Constants.*;
import static graphql.scalars.ExtendedScalars.*;


/**
 * A {@link GraphQLEngine} configured to provide access to the entities and aspects on the the GMS graph.
 */
@Slf4j
public class GmsGraphQLEngine {

    private final EntityClient entityClient;
    private final GraphClient graphClient;
    private final UsageClient usageClient;
    private final SiblingGraphService siblingGraphService;

    private final EntityService entityService;
    private final AnalyticsService analyticsService;
    private final RecommendationsService recommendationsService;
    private final EntityRegistry entityRegistry;
    private final StatefulTokenService statefulTokenService;
    private final SecretService secretService;
    private final GitVersion gitVersion;
    private final boolean supportsImpactAnalysis;
    private final TimeseriesAspectService timeseriesAspectService;
    private final TimelineService timelineService;
    private final NativeUserService nativeUserService;
    private final GroupService groupService;
    private final RoleService roleService;
    private final InviteTokenService inviteTokenService;

    private final FeatureFlags featureFlags;

    private final IngestionConfiguration ingestionConfiguration;
    private final AuthenticationConfiguration authenticationConfiguration;
    private final AuthorizationConfiguration authorizationConfiguration;
    private final VisualConfiguration visualConfiguration;
    private final TelemetryConfiguration telemetryConfiguration;
    private final TestsConfiguration testsConfiguration;
    private final DatahubConfiguration datahubConfiguration;

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
    private final GlossaryNodeType glossaryNodeType;
    private final AspectType aspectType;
    private final ContainerType containerType;
    private final DomainType domainType;
    private final NotebookType notebookType;
    private final AssertionType assertionType;
    private final VersionedDatasetType versionedDatasetType;
    private final DataPlatformInstanceType dataPlatformInstanceType;
    private final AccessTokenMetadataType accessTokenMetadataType;
    private final TestType testType;
    private final DataHubPolicyType dataHubPolicyType;
    private final DataHubRoleType dataHubRoleType;

    /**
     * Configures the graph objects that can be fetched primary key.
     */
    public final List<EntityType<?, ?>> entityTypes;

    /**
     * Configures all graph objects
     */
    public final List<LoadableType<?, ?>> loadableTypes;

    /**
     * Configures the graph objects for owner
     */
    public final List<LoadableType<?, ?>> ownerTypes;

    /**
     * Configures the graph objects that can be searched.
     */
    public final List<SearchableEntityType<?, ?>> searchableTypes;

    /**
     * Configures the graph objects that can be browsed.
     */
    public final List<BrowsableEntityType<?, ?>> browsableTypes;

    public GmsGraphQLEngine(final EntityClient entityClient, final GraphClient graphClient,
        final UsageClient usageClient, final AnalyticsService analyticsService, final EntityService entityService,
        final RecommendationsService recommendationsService, final StatefulTokenService statefulTokenService,
        final TimeseriesAspectService timeseriesAspectService, final EntityRegistry entityRegistry,
        final SecretService secretService, final NativeUserService nativeUserService,
        final IngestionConfiguration ingestionConfiguration,
        final AuthenticationConfiguration authenticationConfiguration,
        final AuthorizationConfiguration authorizationConfiguration, final GitVersion gitVersion,
        final TimelineService timelineService, final boolean supportsImpactAnalysis,
        final VisualConfiguration visualConfiguration, final TelemetryConfiguration telemetryConfiguration,
        final TestsConfiguration testsConfiguration, final DatahubConfiguration datahubConfiguration,
        final SiblingGraphService siblingGraphService, final GroupService groupService, final RoleService roleService,
        final InviteTokenService inviteTokenService, final FeatureFlags featureFlags) {

        this.entityClient = entityClient;
        this.graphClient = graphClient;
        this.usageClient = usageClient;
        this.siblingGraphService = siblingGraphService;

        this.analyticsService = analyticsService;
        this.entityService = entityService;
        this.recommendationsService = recommendationsService;
        this.statefulTokenService = statefulTokenService;
        this.secretService = secretService;
        this.entityRegistry = entityRegistry;
        this.gitVersion = gitVersion;
        this.supportsImpactAnalysis = supportsImpactAnalysis;
        this.timeseriesAspectService = timeseriesAspectService;
        this.timelineService = timelineService;
        this.nativeUserService = nativeUserService;
        this.groupService = groupService;
        this.roleService = roleService;
        this.inviteTokenService = inviteTokenService;

        this.ingestionConfiguration = Objects.requireNonNull(ingestionConfiguration);
        this.authenticationConfiguration = Objects.requireNonNull(authenticationConfiguration);
        this.authorizationConfiguration = Objects.requireNonNull(authorizationConfiguration);
        this.visualConfiguration = visualConfiguration;
        this.telemetryConfiguration = telemetryConfiguration;
        this.testsConfiguration = testsConfiguration;
        this.datahubConfiguration = datahubConfiguration;
        this.featureFlags = featureFlags;

        this.datasetType = new DatasetType(entityClient);
        this.corpUserType = new CorpUserType(entityClient, featureFlags);
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
        this.glossaryNodeType = new GlossaryNodeType(entityClient);
        this.aspectType = new AspectType(entityClient);
        this.containerType = new ContainerType(entityClient);
        this.domainType = new DomainType(entityClient);
        this.notebookType = new NotebookType(entityClient);
        this.assertionType = new AssertionType(entityClient);
        this.versionedDatasetType = new VersionedDatasetType(entityClient);
        this.dataPlatformInstanceType = new DataPlatformInstanceType(entityClient);
        this.accessTokenMetadataType = new AccessTokenMetadataType(entityClient);
        this.testType = new TestType(entityClient);
        this.dataHubPolicyType = new DataHubPolicyType(entityClient);
        this.dataHubRoleType = new DataHubRoleType(entityClient);
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
            glossaryNodeType,
            containerType,
            notebookType,
            domainType,
            assertionType,
            versionedDatasetType,
            dataPlatformInstanceType,
            accessTokenMetadataType,
            testType,
            dataHubPolicyType,
            dataHubRoleType
        );
        this.loadableTypes = new ArrayList<>(entityTypes);
        this.ownerTypes = ImmutableList.of(corpUserType, corpGroupType);
        this.searchableTypes = loadableTypes.stream()
            .filter(type -> (type instanceof SearchableEntityType<?, ?>))
            .map(type -> (SearchableEntityType<?, ?>) type)
            .collect(Collectors.toList());
        this.browsableTypes = loadableTypes.stream()
            .filter(type -> (type instanceof BrowsableEntityType<?, ?>))
            .map(type -> (BrowsableEntityType<?, ?>) type)
            .collect(Collectors.toList());
    }

    /**
     * Returns a {@link Supplier} responsible for creating a new {@link DataLoader} from
     * a {@link LoadableType}.
     */
    public Map<String, Function<QueryContext, DataLoader<?, ?>>> loaderSuppliers(final List<LoadableType<?, ?>> loadableTypes) {
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
        configureGlossaryTermAssociationResolver(builder);
        configureDataJobResolvers(builder);
        configureDataFlowResolvers(builder);
        configureMLFeatureTableResolvers(builder);
        configureGlossaryRelationshipResolvers(builder);
        configureIngestionSourceResolvers(builder);
        configureAnalyticsResolvers(builder);
        configureContainerResolvers(builder);
        configureDataPlatformInstanceResolvers(builder);
        configureGlossaryTermResolvers(builder);
        configureGlossaryNodeResolvers(builder);
        configureDomainResolvers(builder);
        configureAssertionResolvers(builder);
        configurePolicyResolvers(builder);
        configureDataProcessInstanceResolvers(builder);
        configureVersionedDatasetResolvers(builder);
        configureAccessAccessTokenMetadataResolvers(builder);
        configureTestResultResolvers(builder);
        configureRoleResolvers(builder);
    }

    public GraphQLEngine.Builder builder() {
        return GraphQLEngine.builder()
            .addSchema(fileBasedSchema(GMS_SCHEMA_FILE))
            .addSchema(fileBasedSchema(SEARCH_SCHEMA_FILE))
            .addSchema(fileBasedSchema(APP_SCHEMA_FILE))
            .addSchema(fileBasedSchema(AUTH_SCHEMA_FILE))
            .addSchema(fileBasedSchema(ANALYTICS_SCHEMA_FILE))
            .addSchema(fileBasedSchema(RECOMMENDATIONS_SCHEMA_FILE))
            .addSchema(fileBasedSchema(INGESTION_SCHEMA_FILE))
            .addSchema(fileBasedSchema(TIMELINE_SCHEMA_FILE))
            .addSchema(fileBasedSchema(TESTS_SCHEMA_FILE))
            .addDataLoaders(loaderSuppliers(loadableTypes))
            .addDataLoader("Aspect", context -> createDataLoader(aspectType, context))
            .configureRuntimeWiring(this::configureRuntimeWiring);
    }

    public static String fileBasedSchema(String fileName) {
        String schema;
        try {
            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
            schema = IOUtils.toString(is, StandardCharsets.UTF_8);
            is.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find GraphQL Schema with name " + fileName, e);
        }
        return schema;
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
                .dataFetcher("parentContainers", new ParentContainersResolver(entityClient))
                .dataFetcher("dataPlatformInstance",
                    new LoadableTypeResolver<>(dataPlatformInstanceType,
                        (env) -> {
                            final Container container = env.getSource();
                            return container.getDataPlatformInstance() != null ? container.getDataPlatformInstance().getUrn() : null;
                        })
                )
            );
    }

    private void configureDataPlatformInstanceResolvers(final RuntimeWiring.Builder builder) {
        builder
            .type("DataPlatformInstance", typeWiring -> typeWiring
                .dataFetcher("platform",
                    new LoadableTypeResolver<>(dataPlatformType,
                        (env) -> ((DataPlatformInstance) env.getSource()).getPlatform().getUrn()))
            );
    }

    private void configureQueryResolvers(final RuntimeWiring.Builder builder) {
        builder.type("Query", typeWiring -> typeWiring
            .dataFetcher("appConfig",
                new AppConfigResolver(gitVersion, analyticsService != null,
                    this.ingestionConfiguration,
                    this.authenticationConfiguration,
                    this.authorizationConfiguration,
                    this.supportsImpactAnalysis,
                    this.visualConfiguration,
                    this.telemetryConfiguration,
                    this.testsConfiguration,
                    this.datahubConfiguration
            ))
            .dataFetcher("me", new MeResolver(this.entityClient, featureFlags))
            .dataFetcher("search", new SearchResolver(this.entityClient))
            .dataFetcher("searchAcrossEntities", new SearchAcrossEntitiesResolver(this.entityClient))
            .dataFetcher("searchAcrossLineage", new SearchAcrossLineageResolver(this.entityClient))
            .dataFetcher("autoComplete", new AutoCompleteResolver(searchableTypes))
            .dataFetcher("autoCompleteForMultiple", new AutoCompleteForMultipleResolver(searchableTypes))
            .dataFetcher("browse", new BrowseResolver(browsableTypes))
            .dataFetcher("browsePaths", new BrowsePathsResolver(browsableTypes))
            .dataFetcher("dataset", getResolver(datasetType))
            .dataFetcher("versionedDataset", getResolver(versionedDatasetType,
                (env) -> new VersionedUrn().setUrn(UrnUtils.getUrn(env.getArgument(URN_FIELD_NAME)))
                    .setVersionStamp(env.getArgument(VERSION_STAMP_FIELD_NAME))))
            .dataFetcher("notebook", getResolver(notebookType))
            .dataFetcher("corpUser", getResolver(corpUserType))
            .dataFetcher("corpGroup", getResolver(corpGroupType))
            .dataFetcher("dashboard", getResolver(dashboardType))
            .dataFetcher("chart", getResolver(chartType))
            .dataFetcher("tag", getResolver(tagType))
            .dataFetcher("dataFlow", getResolver(dataFlowType))
            .dataFetcher("dataJob", getResolver(dataJobType))
            .dataFetcher("glossaryTerm", getResolver(glossaryTermType))
            .dataFetcher("glossaryNode", getResolver(glossaryNodeType))
            .dataFetcher("domain", getResolver((domainType)))
            .dataFetcher("dataPlatform", getResolver(dataPlatformType))
            .dataFetcher("mlFeatureTable", getResolver(mlFeatureTableType))
            .dataFetcher("mlFeature", getResolver(mlFeatureType))
            .dataFetcher("mlPrimaryKey", getResolver(mlPrimaryKeyType))
            .dataFetcher("mlModel", getResolver(mlModelType))
            .dataFetcher("mlModelGroup", getResolver(mlModelGroupType))
            .dataFetcher("assertion", getResolver(assertionType))
            .dataFetcher("listPolicies", new ListPoliciesResolver(this.entityClient))
            .dataFetcher("getGrantedPrivileges", new GetGrantedPrivilegesResolver())
            .dataFetcher("listUsers", new ListUsersResolver(this.entityClient))
            .dataFetcher("listGroups", new ListGroupsResolver(this.entityClient))
            .dataFetcher("listRecommendations", new ListRecommendationsResolver(recommendationsService))
            .dataFetcher("getEntityCounts", new EntityCountsResolver(this.entityClient))
            .dataFetcher("getAccessToken", new GetAccessTokenResolver(statefulTokenService))
            .dataFetcher("listAccessTokens", new ListAccessTokensResolver(this.entityClient))
            .dataFetcher("container", getResolver(containerType))
            .dataFetcher("listDomains", new ListDomainsResolver(this.entityClient))
            .dataFetcher("listSecrets", new ListSecretsResolver(this.entityClient))
            .dataFetcher("getSecretValues", new GetSecretValuesResolver(this.entityClient, this.secretService))
            .dataFetcher("listIngestionSources", new ListIngestionSourcesResolver(this.entityClient))
            .dataFetcher("ingestionSource", new GetIngestionSourceResolver(this.entityClient))
            .dataFetcher("executionRequest", new GetIngestionExecutionRequestResolver(this.entityClient))
            .dataFetcher("getSchemaBlame", new GetSchemaBlameResolver(this.timelineService))
            .dataFetcher("getSchemaVersionList", new GetSchemaVersionListResolver(this.timelineService))
            .dataFetcher("test", getResolver(testType))
            .dataFetcher("listTests", new ListTestsResolver(entityClient))
            .dataFetcher("getRootGlossaryTerms", new GetRootGlossaryTermsResolver(this.entityClient))
            .dataFetcher("getRootGlossaryNodes", new GetRootGlossaryNodesResolver(this.entityClient))
            .dataFetcher("entityExists", new EntityExistsResolver(this.entityService))
            .dataFetcher("entity", getEntityResolver())
            .dataFetcher("entities", getEntitiesResolver())
            .dataFetcher("listRoles", new ListRolesResolver(this.entityClient))
            .dataFetcher("getInviteToken", new GetInviteTokenResolver(this.inviteTokenService))
        );
    }

    private DataFetcher getEntitiesResolver() {
        return new BatchGetEntitiesResolver(entityTypes,
            (env) -> {
                List<String> urns = env.getArgument(URNS_FIELD_NAME);
                return urns.stream().map((urn) -> {
                    try {
                        Urn entityUrn = Urn.createFromString(urn);
                        return UrnToEntityMapper.map(entityUrn);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to get entity", e);
                    }
                }).collect(Collectors.toList());
            });
    }

    private DataFetcher getEntityResolver() {
        return new EntityTypeResolver(entityTypes,
            (env) -> {
                try {
                    Urn urn = Urn.createFromString(env.getArgument(URN_FIELD_NAME));
                    return UrnToEntityMapper.map(urn);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to get entity", e);
                }
            });
    }

    private DataFetcher getResolver(LoadableType<?, String> loadableType) {
        return getResolver(loadableType, this::getUrnField);
    }

    private <T, K> DataFetcher getResolver(LoadableType<T, K> loadableType,
        Function<DataFetchingEnvironment, K> keyProvider) {
        return new LoadableTypeResolver<>(loadableType, keyProvider);
    }

    private String getUrnField(DataFetchingEnvironment env) {
        return env.getArgument(URN_FIELD_NAME);
    }

    private void configureMutationResolvers(final RuntimeWiring.Builder builder) {
        builder.type("Mutation", typeWiring -> typeWiring
            .dataFetcher("updateDataset", new MutableTypeResolver<>(datasetType))
            .dataFetcher("updateDatasets", new MutableTypeBatchResolver<>(datasetType))
            .dataFetcher("createTag", new CreateTagResolver(this.entityClient, this.entityService))
            .dataFetcher("updateTag", new MutableTypeResolver<>(tagType))
            .dataFetcher("setTagColor", new SetTagColorResolver(entityClient, entityService))
            .dataFetcher("deleteTag", new DeleteTagResolver(entityClient))
            .dataFetcher("updateChart", new MutableTypeResolver<>(chartType))
            .dataFetcher("updateDashboard", new MutableTypeResolver<>(dashboardType))
            .dataFetcher("updateNotebook", new MutableTypeResolver<>(notebookType))
            .dataFetcher("updateDataJob", new MutableTypeResolver<>(dataJobType))
            .dataFetcher("updateDataFlow", new MutableTypeResolver<>(dataFlowType))
            .dataFetcher("updateCorpUserProperties", new MutableTypeResolver<>(corpUserType))
            .dataFetcher("updateCorpGroupProperties", new MutableTypeResolver<>(corpGroupType))
            .dataFetcher("addTag", new AddTagResolver(entityService))
            .dataFetcher("addTags", new AddTagsResolver(entityService))
            .dataFetcher("batchAddTags", new BatchAddTagsResolver(entityService))
            .dataFetcher("removeTag", new RemoveTagResolver(entityService))
            .dataFetcher("batchRemoveTags", new BatchRemoveTagsResolver(entityService))
            .dataFetcher("addTerm", new AddTermResolver(entityService))
            .dataFetcher("batchAddTerms", new BatchAddTermsResolver(entityService))
            .dataFetcher("addTerms", new AddTermsResolver(entityService))
            .dataFetcher("removeTerm", new RemoveTermResolver(entityService))
            .dataFetcher("batchRemoveTerms", new BatchRemoveTermsResolver(entityService))
            .dataFetcher("createPolicy", new UpsertPolicyResolver(this.entityClient))
            .dataFetcher("updatePolicy", new UpsertPolicyResolver(this.entityClient))
            .dataFetcher("deletePolicy", new DeletePolicyResolver(this.entityClient))
            .dataFetcher("updateDescription", new UpdateDescriptionResolver(entityService))
            .dataFetcher("addOwner", new AddOwnerResolver(entityService))
            .dataFetcher("addOwners", new AddOwnersResolver(entityService))
            .dataFetcher("batchAddOwners", new BatchAddOwnersResolver(entityService))
            .dataFetcher("removeOwner", new RemoveOwnerResolver(entityService))
            .dataFetcher("batchRemoveOwners", new BatchRemoveOwnersResolver(entityService))
            .dataFetcher("addLink", new AddLinkResolver(entityService))
            .dataFetcher("removeLink", new RemoveLinkResolver(entityService))
            .dataFetcher("addGroupMembers", new AddGroupMembersResolver(this.groupService))
            .dataFetcher("removeGroupMembers", new RemoveGroupMembersResolver(this.groupService))
            .dataFetcher("createGroup", new CreateGroupResolver(this.groupService))
            .dataFetcher("removeUser", new RemoveUserResolver(this.entityClient))
            .dataFetcher("removeGroup", new RemoveGroupResolver(this.entityClient))
            .dataFetcher("updateUserStatus", new UpdateUserStatusResolver(this.entityClient))
            .dataFetcher("createDomain", new CreateDomainResolver(this.entityClient))
            .dataFetcher("deleteDomain", new DeleteDomainResolver(entityClient))
            .dataFetcher("setDomain", new SetDomainResolver(this.entityClient, this.entityService))
            .dataFetcher("batchSetDomain", new BatchSetDomainResolver(this.entityService))
            .dataFetcher("updateDeprecation", new UpdateDeprecationResolver(this.entityClient, this.entityService))
            .dataFetcher("batchUpdateDeprecation", new BatchUpdateDeprecationResolver(entityService))
            .dataFetcher("unsetDomain", new UnsetDomainResolver(this.entityClient, this.entityService))
            .dataFetcher("createSecret", new CreateSecretResolver(this.entityClient, this.secretService))
            .dataFetcher("deleteSecret", new DeleteSecretResolver(this.entityClient))
            .dataFetcher("createAccessToken", new CreateAccessTokenResolver(this.statefulTokenService))
            .dataFetcher("revokeAccessToken", new RevokeAccessTokenResolver(this.entityClient, this.statefulTokenService))
            .dataFetcher("createIngestionSource", new UpsertIngestionSourceResolver(this.entityClient))
            .dataFetcher("updateIngestionSource", new UpsertIngestionSourceResolver(this.entityClient))
            .dataFetcher("deleteIngestionSource", new DeleteIngestionSourceResolver(this.entityClient))
            .dataFetcher("createIngestionExecutionRequest", new CreateIngestionExecutionRequestResolver(this.entityClient, this.ingestionConfiguration))
            .dataFetcher("cancelIngestionExecutionRequest", new CancelIngestionExecutionRequestResolver(this.entityClient))
            .dataFetcher("createTestConnectionRequest", new CreateTestConnectionRequestResolver(this.entityClient, this.ingestionConfiguration))
            .dataFetcher("deleteAssertion", new DeleteAssertionResolver(this.entityClient, this.entityService))
            .dataFetcher("createTest", new CreateTestResolver(this.entityClient))
            .dataFetcher("updateTest", new UpdateTestResolver(this.entityClient))
            .dataFetcher("deleteTest", new DeleteTestResolver(this.entityClient))
            .dataFetcher("reportOperation", new ReportOperationResolver(this.entityClient))
            .dataFetcher("createGlossaryTerm", new CreateGlossaryTermResolver(this.entityClient))
            .dataFetcher("createGlossaryNode", new CreateGlossaryNodeResolver(this.entityClient))
            .dataFetcher("updateParentNode", new UpdateParentNodeResolver(entityService))
            .dataFetcher("deleteGlossaryEntity",
                new DeleteGlossaryEntityResolver(this.entityClient, this.entityService))
            .dataFetcher("updateName", new UpdateNameResolver(entityService))
            .dataFetcher("addRelatedTerms", new AddRelatedTermsResolver(this.entityService))
            .dataFetcher("removeRelatedTerms", new RemoveRelatedTermsResolver(this.entityService))
            .dataFetcher("createNativeUserResetToken", new CreateNativeUserResetTokenResolver(this.nativeUserService))
            .dataFetcher("batchUpdateSoftDeleted", new BatchUpdateSoftDeletedResolver(this.entityService))
            .dataFetcher("updateUserSetting", new UpdateUserSettingResolver(this.entityService))
            .dataFetcher("rollbackIngestion", new RollbackIngestionResolver(this.entityClient))
            .dataFetcher("batchAssignRole", new BatchAssignRoleResolver(this.roleService))
            .dataFetcher("createInviteToken", new CreateInviteTokenResolver(this.inviteTokenService))
            .dataFetcher("acceptRole", new AcceptRoleResolver(this.roleService, this.inviteTokenService))

        );
    }

    private void configureGenericEntityResolvers(final RuntimeWiring.Builder builder) {
        builder
            .type("SearchResult", typeWiring -> typeWiring
                .dataFetcher("entity", new EntityTypeResolver(entityTypes,
                        (env) -> ((SearchResult) env.getSource()).getEntity()))
                )
            .type("SearchAcrossLineageResult", typeWiring -> typeWiring
                .dataFetcher("entity", new EntityTypeResolver(entityTypes,
                        (env) -> ((SearchAcrossLineageResult) env.getSource()).getEntity()))
            )
            .type("AggregationMetadata", typeWiring -> typeWiring
                .dataFetcher("entity", new EntityTypeResolver(entityTypes,
                    (env) -> ((AggregationMetadata) env.getSource()).getEntity()))
            )
            .type("RecommendationContent", typeWiring -> typeWiring
                .dataFetcher("entity", new EntityTypeResolver(entityTypes,
                    (env) -> ((RecommendationContent) env.getSource()).getEntity()))
            )
            .type("BrowseResults", typeWiring -> typeWiring
                .dataFetcher("entities", new EntityTypeBatchResolver(entityTypes,
                        (env) -> ((BrowseResults) env.getSource()).getEntities()))
            )
            .type("EntityRelationshipLegacy", typeWiring -> typeWiring
                .dataFetcher("entity", new EntityTypeResolver(entityTypes,
                        (env) -> ((EntityRelationshipLegacy) env.getSource()).getEntity()))
            )
            .type("EntityRelationship", typeWiring -> typeWiring
                .dataFetcher("entity", new EntityTypeResolver(entityTypes,
                        (env) -> ((EntityRelationship) env.getSource()).getEntity()))
            )
            .type("LineageRelationship", typeWiring -> typeWiring
                .dataFetcher("entity", new EntityTypeResolver(entityTypes,
                        (env) -> ((LineageRelationship) env.getSource()).getEntity()))
            )
            .type("ListDomainsResult", typeWiring -> typeWiring
                .dataFetcher("domains", new LoadableTypeBatchResolver<>(domainType,
                        (env) -> ((ListDomainsResult) env.getSource()).getDomains().stream()
                            .map(Domain::getUrn)
                            .collect(Collectors.toList())))
            )
            .type("GetRootGlossaryTermsResult", typeWiring -> typeWiring
                .dataFetcher("terms", new LoadableTypeBatchResolver<>(glossaryTermType,
                        (env) -> ((GetRootGlossaryTermsResult) env.getSource()).getTerms().stream()
                            .map(GlossaryTerm::getUrn)
                            .collect(Collectors.toList())))
            )
            .type("GetRootGlossaryNodesResult", typeWiring -> typeWiring
                .dataFetcher("nodes", new LoadableTypeBatchResolver<>(glossaryNodeType,
                        (env) -> ((GetRootGlossaryNodesResult) env.getSource()).getNodes().stream()
                            .map(GlossaryNode::getUrn)
                            .collect(Collectors.toList())))
            )
            .type("AutoCompleteResults", typeWiring -> typeWiring
                .dataFetcher("entities",
                    new EntityTypeBatchResolver(entityTypes,
                        (env) -> ((AutoCompleteResults) env.getSource()).getEntities()))
            )
            .type("AutoCompleteResultForEntity", typeWiring -> typeWiring
                .dataFetcher("entities", new EntityTypeBatchResolver(entityTypes,
                        (env) -> ((AutoCompleteResultForEntity) env.getSource()).getEntities()))
            )
            .type("PolicyMatchCriterionValue", typeWiring -> typeWiring
                .dataFetcher("entity", new EntityTypeResolver(entityTypes,
                        (env) -> ((PolicyMatchCriterionValue) env.getSource()).getEntity()))
            )
            .type("ListTestsResult", typeWiring -> typeWiring
                .dataFetcher("tests", new LoadableTypeBatchResolver<>(testType,
                    (env) -> ((ListTestsResult) env.getSource()).getTests().stream()
                        .map(Test::getUrn)
                        .collect(Collectors.toList())))
            );
    }

    /**
     * Configures resolvers responsible for resolving the {@link com.linkedin.datahub.graphql.generated.Dataset} type.
     */
    private void configureDatasetResolvers(final RuntimeWiring.Builder builder) {
        builder
            .type("Dataset", typeWiring -> typeWiring
                .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient))
                .dataFetcher("browsePaths", new EntityBrowsePathsResolver(this.datasetType))
                .dataFetcher("lineage", new EntityLineageResultResolver(siblingGraphService))
                .dataFetcher("platform", new LoadableTypeResolver<>(dataPlatformType,
                            (env) -> ((Dataset) env.getSource()).getPlatform().getUrn())
                )
                .dataFetcher("container",
                    new LoadableTypeResolver<>(containerType,
                        (env) -> {
                            final Dataset dataset = env.getSource();
                            return dataset.getContainer() != null ? dataset.getContainer().getUrn() : null;
                        })
                )
                .dataFetcher("dataPlatformInstance",
                    new LoadableTypeResolver<>(dataPlatformInstanceType,
                        (env) -> {
                            final Dataset dataset = env.getSource();
                            return dataset.getDataPlatformInstance() != null ? dataset.getDataPlatformInstance().getUrn() : null;
                        })
                )
                .dataFetcher("datasetProfiles", new TimeSeriesAspectResolver(
                        this.entityClient,
                        "dataset",
                        "datasetProfile",
                        DatasetProfileMapper::map
                    )
                )
                .dataFetcher("operations", new TimeSeriesAspectResolver(
                            this.entityClient,
                            "dataset",
                            "operation",
                            OperationMapper::map
                    )
                )
                .dataFetcher("usageStats", new DatasetUsageStatsResolver(this.usageClient))
                .dataFetcher("statsSummary", new DatasetStatsSummaryResolver(this.usageClient))
                .dataFetcher("health", new DatasetHealthResolver(graphClient, timeseriesAspectService))
                .dataFetcher("schemaMetadata", new AspectResolver())
                .dataFetcher("assertions", new EntityAssertionsResolver(entityClient, graphClient))
                .dataFetcher("testResults", new TestResultsResolver(entityClient))
                .dataFetcher("aspects", new WeaklyTypedAspectsResolver(entityClient, entityRegistry))
                .dataFetcher("subTypes", new SubTypesResolver(
                   this.entityClient,
                   "dataset",
                   "subTypes"))
                .dataFetcher("runs", new EntityRunsResolver(entityClient))
                .dataFetcher("parentContainers", new ParentContainersResolver(entityClient)))
            .type("Owner", typeWiring -> typeWiring
                    .dataFetcher("owner", new OwnerTypeResolver<>(ownerTypes,
                        (env) -> ((Owner) env.getSource()).getOwner()))
            )
            .type("UserUsageCounts", typeWiring -> typeWiring
                .dataFetcher("user", new LoadableTypeResolver<>(corpUserType,
                    (env) -> ((UserUsageCounts) env.getSource()).getUser().getUrn()))
            )
            .type("ForeignKeyConstraint", typeWiring -> typeWiring
                .dataFetcher("foreignDataset", new LoadableTypeResolver<>(datasetType,
                    (env) -> ((ForeignKeyConstraint) env.getSource()).getForeignDataset().getUrn()))
            )
            .type("SiblingProperties", typeWiring -> typeWiring
                .dataFetcher("siblings",
                    new EntityTypeBatchResolver(
                        new ArrayList<>(entityTypes),
                        (env) -> ((SiblingProperties) env.getSource()).getSiblings()))
            )
            .type("InstitutionalMemoryMetadata", typeWiring -> typeWiring
                .dataFetcher("author", new LoadableTypeResolver<>(corpUserType,
                    (env) -> ((InstitutionalMemoryMetadata) env.getSource()).getAuthor().getUrn()))
            )
            .type("DatasetStatsSummary", typeWiring -> typeWiring
                .dataFetcher("topUsersLast30Days", new LoadableTypeBatchResolver<>(corpUserType,
                    (env) -> {
                        DatasetStatsSummary summary = ((DatasetStatsSummary) env.getSource());
                        return summary.getTopUsersLast30Days() != null
                            ? summary.getTopUsersLast30Days().stream()
                            .map(CorpUser::getUrn)
                            .collect(Collectors.toList())
                            : null;
                    }))
            );
    }

    /**
     * Configures resolvers responsible for resolving the {@link com.linkedin.datahub.graphql.generated.VersionedDataset} type.
     */
    private void configureVersionedDatasetResolvers(final RuntimeWiring.Builder builder) {
        builder
            .type("VersionedDataset", typeWiring -> typeWiring
                .dataFetcher("relationships", new StaticDataFetcher(null)));

    }

    /**
     * Configures resolvers responsible for resolving the {@link com.linkedin.datahub.graphql.generated.AccessTokenMetadata} type.
     */
    private void configureAccessAccessTokenMetadataResolvers(final RuntimeWiring.Builder builder) {
        builder.type("AccessToken", typeWiring -> typeWiring
            .dataFetcher("metadata", new LoadableTypeResolver<>(accessTokenMetadataType,
                (env) -> ((AccessToken) env.getSource()).getMetadata().getUrn()))
        );
        builder.type("ListAccessTokenResult", typeWiring -> typeWiring
            .dataFetcher("tokens", new LoadableTypeBatchResolver<>(accessTokenMetadataType,
                (env) -> ((ListAccessTokenResult) env.getSource()).getTokens().stream()
                    .map(AccessTokenMetadata::getUrn)
                    .collect(Collectors.toList())))
        );
    }

    private void configureGlossaryTermResolvers(final RuntimeWiring.Builder builder) {
        builder.type("GlossaryTerm", typeWiring -> typeWiring
            .dataFetcher("schemaMetadata", new AspectResolver())
            .dataFetcher("parentNodes", new ParentNodesResolver(entityClient))
        );
    }

    private void configureGlossaryNodeResolvers(final RuntimeWiring.Builder builder) {
        builder.type("GlossaryNode", typeWiring -> typeWiring
            .dataFetcher("parentNodes", new ParentNodesResolver(entityClient))
        );
    }

    /**
     * Configures resolvers responsible for resolving the {@link com.linkedin.datahub.graphql.generated.CorpUser} type.
     */
    private void configureCorpUserResolvers(final RuntimeWiring.Builder builder) {
        builder.type("CorpUser", typeWiring -> typeWiring
            .dataFetcher("relationships",
                new EntityRelationshipsResultResolver(graphClient))
        );
        builder.type("CorpUserInfo", typeWiring -> typeWiring
            .dataFetcher("manager", new LoadableTypeResolver<>(corpUserType,
                (env) -> ((CorpUserInfo) env.getSource()).getManager().getUrn()))
        );
    }

    /**
     * Configures resolvers responsible for resolving the {@link com.linkedin.datahub.graphql.generated.CorpGroup} type.
     */
    private void configureCorpGroupResolvers(final RuntimeWiring.Builder builder) {
        builder.type("CorpGroup", typeWiring -> typeWiring
            .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient)));
        builder.type("CorpGroupInfo", typeWiring -> typeWiring
            .dataFetcher("admins",
                    new LoadableTypeBatchResolver<>(corpUserType,
                            (env) -> ((CorpGroupInfo) env.getSource()).getAdmins().stream()
                                    .map(CorpUser::getUrn)
                                    .collect(Collectors.toList())))
            .dataFetcher("members",
                    new LoadableTypeBatchResolver<>(corpUserType,
                            (env) -> ((CorpGroupInfo) env.getSource()).getMembers().stream()
                                    .map(CorpUser::getUrn)
                                    .collect(Collectors.toList())))
        );
    }

    private void configureTagAssociationResolver(final RuntimeWiring.Builder builder) {
        builder.type("Tag", typeWiring -> typeWiring
            .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient)));
        builder.type("TagAssociation", typeWiring -> typeWiring
            .dataFetcher("tag",
                    new LoadableTypeResolver<>(tagType,
                            (env) -> ((com.linkedin.datahub.graphql.generated.TagAssociation) env.getSource()).getTag().getUrn()))
        );
    }

    private void configureGlossaryTermAssociationResolver(final RuntimeWiring.Builder builder) {
        builder.type("GlossaryTermAssociation", typeWiring -> typeWiring
                .dataFetcher("term",
                    new LoadableTypeResolver<>(glossaryTermType,
                        (env) -> ((GlossaryTermAssociation) env.getSource()).getTerm().getUrn()))
        );
    }

  /**
   * Configures resolvers responsible for resolving the {@link com.linkedin.datahub.graphql.generated.Notebook} type.
   */
  private void configureNotebookResolvers(final RuntimeWiring.Builder builder) {
    builder.type("Notebook", typeWiring -> typeWiring
        .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient))
        .dataFetcher("browsePaths", new EntityBrowsePathsResolver(this.notebookType))
        .dataFetcher("platform", new LoadableTypeResolver<>(dataPlatformType,
            (env) -> ((Notebook) env.getSource()).getPlatform().getUrn()))
        .dataFetcher("dataPlatformInstance",
            new LoadableTypeResolver<>(dataPlatformInstanceType,
                (env) -> {
                  final Notebook notebook = env.getSource();
                  return notebook.getDataPlatformInstance() != null ? notebook.getDataPlatformInstance().getUrn() : null;
                })
        )
    );
  }

    /**
     * Configures resolvers responsible for resolving the {@link com.linkedin.datahub.graphql.generated.Dashboard} type.
     */
    private void configureDashboardResolvers(final RuntimeWiring.Builder builder) {
        builder.type("Dashboard", typeWiring -> typeWiring
            .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient))
            .dataFetcher("browsePaths", new EntityBrowsePathsResolver(this.dashboardType))
            .dataFetcher("lineage", new EntityLineageResultResolver(siblingGraphService))
            .dataFetcher("platform", new LoadableTypeResolver<>(dataPlatformType,
                    (env) -> ((Dashboard) env.getSource()).getPlatform().getUrn()))
            .dataFetcher("dataPlatformInstance",
                new LoadableTypeResolver<>(dataPlatformInstanceType,
                    (env) -> {
                        final Dashboard dashboard = env.getSource();
                        return dashboard.getDataPlatformInstance() != null ? dashboard.getDataPlatformInstance().getUrn() : null;
                    })
            )
            .dataFetcher("container", new LoadableTypeResolver<>(containerType,
                    (env) -> {
                        final Dashboard dashboard = env.getSource();
                        return dashboard.getContainer() != null ? dashboard.getContainer().getUrn() : null;
                    })
            )
            .dataFetcher("parentContainers", new ParentContainersResolver(entityClient))
            .dataFetcher("usageStats", new DashboardUsageStatsResolver(timeseriesAspectService))
            .dataFetcher("statsSummary", new DashboardStatsSummaryResolver(timeseriesAspectService))
        );
        builder.type("DashboardInfo", typeWiring -> typeWiring
            .dataFetcher("charts", new LoadableTypeBatchResolver<>(chartType,
                    (env) -> ((DashboardInfo) env.getSource()).getCharts().stream()
                        .map(Chart::getUrn)
                        .collect(Collectors.toList())))
        );
        builder.type("DashboardUserUsageCounts", typeWiring -> typeWiring
            .dataFetcher("user", new LoadableTypeResolver<>(
                corpUserType,
                (env) -> ((DashboardUserUsageCounts) env.getSource()).getUser().getUrn()))
        );
        builder.type("DashboardStatsSummary", typeWiring -> typeWiring
            .dataFetcher("topUsersLast30Days", new LoadableTypeBatchResolver<>(corpUserType,
                (env) -> {
                DashboardStatsSummary summary = ((DashboardStatsSummary) env.getSource());
                return summary.getTopUsersLast30Days() != null
                    ? summary.getTopUsersLast30Days().stream()
                        .map(CorpUser::getUrn)
                        .collect(Collectors.toList())
                    : null;
                }))
        );
    }

    /**
     * Configures resolvers responsible for resolving the {@link com.linkedin.datahub.graphql.generated.Chart} type.
     */
    private void configureChartResolvers(final RuntimeWiring.Builder builder) {
        builder.type("Chart", typeWiring -> typeWiring
            .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient))
            .dataFetcher("browsePaths", new EntityBrowsePathsResolver(this.chartType))
            .dataFetcher("lineage", new EntityLineageResultResolver(siblingGraphService))
            .dataFetcher("platform", new LoadableTypeResolver<>(dataPlatformType,
                (env) -> ((Chart) env.getSource()).getPlatform().getUrn()))
            .dataFetcher("dataPlatformInstance",
                new LoadableTypeResolver<>(dataPlatformInstanceType,
                    (env) -> {
                        final Chart chart = env.getSource();
                        return chart.getDataPlatformInstance() != null ? chart.getDataPlatformInstance().getUrn() : null;
                    })
            )
            .dataFetcher("container", new LoadableTypeResolver<>(
                containerType,
                (env) -> {
                    final Chart chart = env.getSource();
                    return chart.getContainer() != null ? chart.getContainer().getUrn() : null;
                })
            )
            .dataFetcher("parentContainers", new ParentContainersResolver(entityClient))
            .dataFetcher("statsSummary", new ChartStatsSummaryResolver(this.timeseriesAspectService))
        );
        builder.type("ChartInfo", typeWiring -> typeWiring
            .dataFetcher("inputs", new LoadableTypeBatchResolver<>(datasetType,
                (env) -> ((ChartInfo) env.getSource()).getInputs().stream()
                    .map(datasetType.getKeyProvider())
                    .collect(Collectors.toList())))
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
                        .map(graphType -> (EntityType<?, ?>) graphType)
                        .collect(Collectors.toList())
                )))
            .type("EntityWithRelationships", typeWiring -> typeWiring
                .typeResolver(new EntityInterfaceTypeResolver(loadableTypes.stream()
                        .filter(graphType -> graphType instanceof EntityType)
                        .map(graphType -> (EntityType<?, ?>) graphType)
                        .collect(Collectors.toList())
                )))
            .type("BrowsableEntity", typeWiring -> typeWiring
                .typeResolver(new EntityInterfaceTypeResolver(browsableTypes.stream()
                    .map(graphType -> (EntityType<?, ?>) graphType)
                    .collect(Collectors.toList())
                )))
            .type("OwnerType", typeWiring -> typeWiring
                .typeResolver(new EntityInterfaceTypeResolver(ownerTypes.stream()
                    .filter(graphType -> graphType instanceof EntityType)
                    .map(graphType -> (EntityType<?, ?>) graphType)
                    .collect(Collectors.toList())
                )))
            .type("PlatformSchema", typeWiring -> typeWiring
                    .typeResolver(new PlatformSchemaUnionTypeResolver())
            )
            .type("HyperParameterValueType", typeWiring -> typeWiring
                    .typeResolver(new HyperParameterValueTypeResolver())
            )
            .type("Aspect", typeWiring -> typeWiring.typeResolver(new AspectInterfaceTypeResolver()))
            .type("TimeSeriesAspect", typeWiring -> typeWiring
                .typeResolver(new TimeSeriesAspectInterfaceTypeResolver()))
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
                .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient))
                .dataFetcher("browsePaths", new EntityBrowsePathsResolver(this.dataJobType))
                .dataFetcher("lineage", new EntityLineageResultResolver(siblingGraphService))
                .dataFetcher("dataFlow", new LoadableTypeResolver<>(dataFlowType,
                    (env) -> ((DataJob) env.getSource()).getDataFlow().getUrn()))
                .dataFetcher("dataPlatformInstance",
                    new LoadableTypeResolver<>(dataPlatformInstanceType,
                        (env) -> {
                            final DataJob dataJob = env.getSource();
                            return dataJob.getDataPlatformInstance() != null ? dataJob.getDataPlatformInstance().getUrn() : null;
                        })
                )
                .dataFetcher("runs", new DataJobRunsResolver(entityClient))
            )
            .type("DataJobInputOutput", typeWiring -> typeWiring
                .dataFetcher("inputDatasets", new LoadableTypeBatchResolver<>(datasetType,
                    (env) -> ((DataJobInputOutput) env.getSource()).getInputDatasets().stream()
                        .map(datasetType.getKeyProvider())
                        .collect(Collectors.toList())))
                .dataFetcher("outputDatasets", new LoadableTypeBatchResolver<>(datasetType,
                    (env) -> ((DataJobInputOutput) env.getSource()).getOutputDatasets().stream()
                        .map(datasetType.getKeyProvider())
                        .collect(Collectors.toList())))
                .dataFetcher("inputDatajobs", new LoadableTypeBatchResolver<>(dataJobType,
                    (env) -> ((DataJobInputOutput) env.getSource()).getInputDatajobs().stream()
                        .map(DataJob::getUrn)
                        .collect(Collectors.toList())))
            );
    }

    /**
     * Configures resolvers responsible for resolving the {@link com.linkedin.datahub.graphql.generated.DataFlow} type.
     */
    private void configureDataFlowResolvers(final RuntimeWiring.Builder builder) {
        builder
            .type("DataFlow", typeWiring -> typeWiring
                .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient))
                .dataFetcher("browsePaths", new EntityBrowsePathsResolver(this.dataFlowType))
                .dataFetcher("lineage", new EntityLineageResultResolver(siblingGraphService))
                .dataFetcher("platform", new LoadableTypeResolver<>(dataPlatformType,
                    (env) -> ((DataFlow) env.getSource()).getPlatform().getUrn()))
                .dataFetcher("dataPlatformInstance",
                    new LoadableTypeResolver<>(dataPlatformInstanceType,
                        (env) -> {
                            final DataFlow dataFlow = env.getSource();
                            return dataFlow.getDataPlatformInstance() != null ? dataFlow.getDataPlatformInstance().getUrn() : null;
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
                .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient))
                .dataFetcher("browsePaths", new EntityBrowsePathsResolver(this.mlFeatureTableType))
                .dataFetcher("lineage", new EntityLineageResultResolver(siblingGraphService))
                .dataFetcher("platform",
                        new LoadableTypeResolver<>(dataPlatformType,
                                (env) -> ((MLFeatureTable) env.getSource()).getPlatform().getUrn()))
                .dataFetcher("dataPlatformInstance",
                    new LoadableTypeResolver<>(dataPlatformInstanceType,
                        (env) -> {
                            final MLFeatureTable entity = env.getSource();
                            return entity.getDataPlatformInstance() != null ? entity.getDataPlatformInstance().getUrn() : null;
                        })
                )
            )
            .type("MLFeatureTableProperties", typeWiring -> typeWiring
                .dataFetcher("mlFeatures",
                                new LoadableTypeBatchResolver<>(mlFeatureType,
                                        (env) ->
                                            ((MLFeatureTableProperties) env.getSource()).getMlFeatures() != null
                                                ? ((MLFeatureTableProperties) env.getSource()).getMlFeatures().stream()
                                        .map(MLFeature::getUrn)
                                        .collect(Collectors.toList()) : ImmutableList.of()))
                .dataFetcher("mlPrimaryKeys",
                                new LoadableTypeBatchResolver<>(mlPrimaryKeyType,
                                        (env) ->
                                            ((MLFeatureTableProperties) env.getSource()).getMlPrimaryKeys() != null
                                                ? ((MLFeatureTableProperties) env.getSource()).getMlPrimaryKeys().stream()
                                        .map(MLPrimaryKey::getUrn)
                                        .collect(Collectors.toList()) : ImmutableList.of()))
            )
            .type("MLFeatureProperties", typeWiring -> typeWiring
                .dataFetcher("sources", new LoadableTypeBatchResolver<>(datasetType,
                                (env) -> {
                                    if (((MLFeatureProperties) env.getSource()).getSources() == null) {
                                        return Collections.emptyList();
                                    }
                                    return ((MLFeatureProperties) env.getSource()).getSources().stream()
                                                        .map(datasetType.getKeyProvider())
                                                        .collect(Collectors.toList());
                                    })
                )
            )
            .type("MLPrimaryKeyProperties", typeWiring -> typeWiring
                .dataFetcher("sources", new LoadableTypeBatchResolver<>(datasetType,
                                (env) -> {
                                    if (((MLPrimaryKeyProperties) env.getSource()).getSources() == null) {
                                        return Collections.emptyList();
                                    }
                                    return ((MLPrimaryKeyProperties) env.getSource()).getSources().stream()
                                        .map(datasetType.getKeyProvider())
                                        .collect(Collectors.toList());
                                })
                )
            )
            .type("MLModel", typeWiring -> typeWiring
                .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient))
                .dataFetcher("browsePaths", new EntityBrowsePathsResolver(this.mlModelType))
                .dataFetcher("lineage", new EntityLineageResultResolver(siblingGraphService))
                .dataFetcher("platform", new LoadableTypeResolver<>(dataPlatformType,
                    (env) -> ((MLModel) env.getSource()).getPlatform().getUrn()))
                .dataFetcher("dataPlatformInstance",
                    new LoadableTypeResolver<>(dataPlatformInstanceType,
                        (env) -> {
                            final MLModel mlModel = env.getSource();
                            return mlModel.getDataPlatformInstance() != null ? mlModel.getDataPlatformInstance().getUrn() : null;
                        })
                )
            )
            .type("MLModelProperties", typeWiring -> typeWiring
                .dataFetcher("groups", new LoadableTypeBatchResolver<>(mlModelGroupType,
                        (env) -> {
                            MLModelProperties properties = env.getSource();
                            if (properties.getGroups() != null) {
                                return properties.getGroups().stream()
                                    .map(MLModelGroup::getUrn)
                                    .collect(Collectors.toList());
                            }
                            return Collections.emptyList();
                        })
                )
            )
            .type("MLModelGroup", typeWiring -> typeWiring
                .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient))
                .dataFetcher("browsePaths", new EntityBrowsePathsResolver(this.mlModelGroupType))
                .dataFetcher("lineage", new EntityLineageResultResolver(siblingGraphService))
                .dataFetcher("platform", new LoadableTypeResolver<>(dataPlatformType,
                                (env) -> ((MLModelGroup) env.getSource()).getPlatform().getUrn())
                )
                .dataFetcher("dataPlatformInstance",
                    new LoadableTypeResolver<>(dataPlatformInstanceType,
                        (env) -> {
                            final MLModelGroup entity = env.getSource();
                            return entity.getDataPlatformInstance() != null ? entity.getDataPlatformInstance().getUrn() : null;
                        })
                )
            )
            .type("MLFeature", typeWiring -> typeWiring
                .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient))
                .dataFetcher("lineage",  new EntityLineageResultResolver(siblingGraphService))
                .dataFetcher("dataPlatformInstance",
                    new LoadableTypeResolver<>(dataPlatformInstanceType,
                        (env) -> {
                            final MLFeature entity = env.getSource();
                            return entity.getDataPlatformInstance() != null ? entity.getDataPlatformInstance().getUrn() : null;
                        })
                )
            )
            .type("MLPrimaryKey", typeWiring -> typeWiring
                .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient))
                .dataFetcher("lineage", new EntityLineageResultResolver(siblingGraphService))
                .dataFetcher("dataPlatformInstance",
                    new LoadableTypeResolver<>(dataPlatformInstanceType,
                        (env) -> {
                            final MLPrimaryKey entity = env.getSource();
                            return entity.getDataPlatformInstance() != null ? entity.getDataPlatformInstance().getUrn() : null;
                        })
                )
            );
    }

    private void configureGlossaryRelationshipResolvers(final RuntimeWiring.Builder builder) {
        builder.type("GlossaryTerm", typeWiring -> typeWiring.dataFetcher("relationships",
            new EntityRelationshipsResultResolver(graphClient)))
        .type("GlossaryNode", typeWiring -> typeWiring.dataFetcher("relationships",
            new EntityRelationshipsResultResolver(graphClient)));
    }

    private void configureDomainResolvers(final RuntimeWiring.Builder builder) {
        builder.type("Domain", typeWiring -> typeWiring
            .dataFetcher("entities", new DomainEntitiesResolver(this.entityClient))
            .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient)
            )
        );
        builder.type("DomainAssociation", typeWiring -> typeWiring
            .dataFetcher("domain",
                new LoadableTypeResolver<>(domainType,
                    (env) -> ((com.linkedin.datahub.graphql.generated.DomainAssociation) env.getSource()).getDomain().getUrn()))
        );
    }

    private void configureAssertionResolvers(final RuntimeWiring.Builder builder) {
        builder.type("Assertion", typeWiring -> typeWiring.dataFetcher("relationships",
                new EntityRelationshipsResultResolver(graphClient))
            .dataFetcher("platform", new LoadableTypeResolver<>(dataPlatformType,
                (env) -> ((Assertion) env.getSource()).getPlatform().getUrn()))
            .dataFetcher("dataPlatformInstance",
                new LoadableTypeResolver<>(dataPlatformInstanceType,
                    (env) -> {
                        final Assertion assertion = env.getSource();
                        return assertion.getDataPlatformInstance() != null ? assertion.getDataPlatformInstance().getUrn() : null;
                    })
            )
            .dataFetcher("runEvents", new AssertionRunEventResolver(entityClient)));
    }

    private void configurePolicyResolvers(final RuntimeWiring.Builder builder) {
        // Register resolvers for "resolvedUsers" and "resolvedGroups" field of the Policy type.
        builder.type("ActorFilter", typeWiring -> typeWiring.dataFetcher("resolvedUsers",
            new LoadableTypeBatchResolver<>(corpUserType, (env) -> {
                final ActorFilter filter = env.getSource();
                return filter.getUsers();
            })).dataFetcher("resolvedGroups", new LoadableTypeBatchResolver<>(corpGroupType, (env) -> {
            final ActorFilter filter = env.getSource();
            return filter.getGroups();
        })).dataFetcher("resolvedRoles", new LoadableTypeBatchResolver<>(dataHubRoleType, (env) -> {
            final ActorFilter filter = env.getSource();
            return filter.getRoles();
        })));
    }

    private void configureRoleResolvers(final RuntimeWiring.Builder builder) {
        builder.type("DataHubRole",
            typeWiring -> typeWiring.dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient)));
    }

    private void configureDataProcessInstanceResolvers(final RuntimeWiring.Builder builder) {
        builder.type("DataProcessInstance",
            typeWiring -> typeWiring.dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient))
                .dataFetcher("lineage", new EntityLineageResultResolver(siblingGraphService))
                .dataFetcher("state", new TimeSeriesAspectResolver(this.entityClient, "dataProcessInstance",
                    DATA_PROCESS_INSTANCE_RUN_EVENT_ASPECT_NAME, DataProcessInstanceRunEventMapper::map)));
    }

    private void configureTestResultResolvers(final RuntimeWiring.Builder builder) {
        builder.type("TestResult", typeWiring -> typeWiring
            .dataFetcher("test", new LoadableTypeResolver<>(testType,
                (env) -> {
                    final TestResult testResult = env.getSource();
                    return testResult.getTest() != null ? testResult.getTest().getUrn() : null;
                }))
        );
    }

    private <T, K> DataLoader<K, DataFetcherResult<T>> createDataLoader(final LoadableType<T, K> graphType, final QueryContext queryContext) {
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
        builder.type("IngestionSource", typeWiring -> typeWiring
            .dataFetcher("executions", new IngestionSourceExecutionRequestsResolver(entityClient))
            .dataFetcher("platform", new LoadableTypeResolver<>(dataPlatformType,
                (env) -> {
                    final IngestionSource ingestionSource = env.getSource();
                    return ingestionSource.getPlatform() != null ? ingestionSource.getPlatform().getUrn() : null;
                })
            ));
    }
}
