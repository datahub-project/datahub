package com.linkedin.datahub.graphql.plugins;

import static com.linkedin.datahub.graphql.AcrylConstants.*;

import com.datahub.authentication.group.GroupService;
import com.datahub.authentication.proposal.ProposalService;
import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.GmsGraphQLEngine;
import com.linkedin.datahub.graphql.GmsGraphQLEngineArgs;
import com.linkedin.datahub.graphql.GmsGraphQLPlugin;
import com.linkedin.datahub.graphql.WeaklyTypedAspectsResolver;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.datahub.graphql.resolvers.action.execution.ListActionPipelineResolver;
import com.linkedin.datahub.graphql.resolvers.action.execution.UpsertActionPipelineResolver;
import com.linkedin.datahub.graphql.resolvers.actionrequest.ListActionRequestsResolver;
import com.linkedin.datahub.graphql.resolvers.actionrequest.ListRejectedActionRequestsResolver;
import com.linkedin.datahub.graphql.resolvers.ai.SuggestDescriptionResolver;
import com.linkedin.datahub.graphql.resolvers.anomaly.EntityAnomaliesResolver;
import com.linkedin.datahub.graphql.resolvers.assertion.CreateDatasetAssertionResolver;
import com.linkedin.datahub.graphql.resolvers.assertion.CreateFieldAssertionResolver;
import com.linkedin.datahub.graphql.resolvers.assertion.CreateFreshnessAssertionResolver;
import com.linkedin.datahub.graphql.resolvers.assertion.CreateSqlAssertionResolver;
import com.linkedin.datahub.graphql.resolvers.assertion.CreateVolumeAssertionResolver;
import com.linkedin.datahub.graphql.resolvers.assertion.EntityAssertionsResolver;
import com.linkedin.datahub.graphql.resolvers.assertion.TestAssertionResolver;
import com.linkedin.datahub.graphql.resolvers.assertion.UpdateAssertionMetadataResolver;
import com.linkedin.datahub.graphql.resolvers.assertion.UpdateDatasetAssertionResolver;
import com.linkedin.datahub.graphql.resolvers.assertion.UpsertDatasetFieldAssertionMonitorResolver;
import com.linkedin.datahub.graphql.resolvers.assertion.UpsertDatasetFreshnessAssertionMonitorResolver;
import com.linkedin.datahub.graphql.resolvers.assertion.UpsertDatasetSqlAssertionMonitorResolver;
import com.linkedin.datahub.graphql.resolvers.assertion.UpsertDatasetVolumeAssertionMonitorResolver;
import com.linkedin.datahub.graphql.resolvers.connection.UpsertConnectionResolver;
import com.linkedin.datahub.graphql.resolvers.constraint.ConstraintsResolver;
import com.linkedin.datahub.graphql.resolvers.constraint.CreateTermConstraintResolver;
import com.linkedin.datahub.graphql.resolvers.datacontract.EntityDataContractResolver;
import com.linkedin.datahub.graphql.resolvers.datacontract.UpsertDataContractResolver;
import com.linkedin.datahub.graphql.resolvers.dataset.DatasetStatsSummaryResolver;
import com.linkedin.datahub.graphql.resolvers.form.BatchSubmitFormPromptResolver;
import com.linkedin.datahub.graphql.resolvers.form.BatchVerifyFormResolver;
import com.linkedin.datahub.graphql.resolvers.form.FormAnalyticsConfigResolver;
import com.linkedin.datahub.graphql.resolvers.form.FormAnalyticsResolver;
import com.linkedin.datahub.graphql.resolvers.form.GetFormsForActorResolver;
import com.linkedin.datahub.graphql.resolvers.form.NumEntitiesToCompleteResolver;
import com.linkedin.datahub.graphql.resolvers.incident.UpdateIncidentResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.credentials.ListExecutorConfigsResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.execution.ListSignalRequestsResolver;
import com.linkedin.datahub.graphql.resolvers.integration.GetLinkPreviewResolver;
import com.linkedin.datahub.graphql.resolvers.load.*;
import com.linkedin.datahub.graphql.resolvers.monitor.CreateAssertionMonitorResolver;
import com.linkedin.datahub.graphql.resolvers.monitor.DeleteMonitorResolver;
import com.linkedin.datahub.graphql.resolvers.monitor.SystemMonitorsResolver;
import com.linkedin.datahub.graphql.resolvers.monitor.UpdateMonitorStatusResolver;
import com.linkedin.datahub.graphql.resolvers.monitor.UpdateSystemMonitorsResolver;
import com.linkedin.datahub.graphql.resolvers.proposal.AcceptProposalResolver;
import com.linkedin.datahub.graphql.resolvers.proposal.ProposeCreateGlossaryNodeResolver;
import com.linkedin.datahub.graphql.resolvers.proposal.ProposeCreateGlossaryTermResolver;
import com.linkedin.datahub.graphql.resolvers.proposal.ProposeDataContractResolver;
import com.linkedin.datahub.graphql.resolvers.proposal.ProposeTagResolver;
import com.linkedin.datahub.graphql.resolvers.proposal.ProposeTermResolver;
import com.linkedin.datahub.graphql.resolvers.proposal.ProposeUpdateDescriptionResolver;
import com.linkedin.datahub.graphql.resolvers.proposal.RejectProposalResolver;
import com.linkedin.datahub.graphql.resolvers.role.BatchAssignRoleResolver;
import com.linkedin.datahub.graphql.resolvers.settings.GlobalSettingsResolver;
import com.linkedin.datahub.graphql.resolvers.settings.UpdateGlobalSettingsResolver;
import com.linkedin.datahub.graphql.resolvers.settings.UpdateHelpLinkResolver;
import com.linkedin.datahub.graphql.resolvers.settings.group.GetGroupNotificationSettingsResolver;
import com.linkedin.datahub.graphql.resolvers.settings.group.UpdateGroupNotificationSettingsResolver;
import com.linkedin.datahub.graphql.resolvers.settings.user.GetUserNotificationSettingsResolver;
import com.linkedin.datahub.graphql.resolvers.settings.user.UpdateUserNotificationSettingsResolver;
import com.linkedin.datahub.graphql.resolvers.share.ShareEntityResolver;
import com.linkedin.datahub.graphql.resolvers.share.UnshareEntityResolver;
import com.linkedin.datahub.graphql.resolvers.subscription.CreateSubscriptionResolver;
import com.linkedin.datahub.graphql.resolvers.subscription.DeleteSubscriptionResolver;
import com.linkedin.datahub.graphql.resolvers.subscription.GetEntitySubscriptionSummaryResolver;
import com.linkedin.datahub.graphql.resolvers.subscription.GetSubscriptionResolver;
import com.linkedin.datahub.graphql.resolvers.subscription.ListSubscriptionsResolver;
import com.linkedin.datahub.graphql.resolvers.subscription.UpdateSubscriptionResolver;
import com.linkedin.datahub.graphql.resolvers.test.BatchTestRunEventsResolver;
import com.linkedin.datahub.graphql.resolvers.test.CreateTestResolver;
import com.linkedin.datahub.graphql.resolvers.test.DeleteTestResolver;
import com.linkedin.datahub.graphql.resolvers.test.EntityTestResultsResolver;
import com.linkedin.datahub.graphql.resolvers.test.RunTestDefinitionResolver;
import com.linkedin.datahub.graphql.resolvers.test.RunTestsResolver;
import com.linkedin.datahub.graphql.resolvers.test.TestResultsSummaryResolver;
import com.linkedin.datahub.graphql.resolvers.test.UpdateTestResolver;
import com.linkedin.datahub.graphql.resolvers.test.ValidateTestResolver;
import com.linkedin.datahub.graphql.types.EntityType;
import com.linkedin.datahub.graphql.types.LoadableType;
import com.linkedin.datahub.graphql.types.action.ActionPipelineType;
import com.linkedin.datahub.graphql.types.anomaly.AnomalyType;
import com.linkedin.datahub.graphql.types.connection.DataHubConnectionType;
import com.linkedin.datahub.graphql.types.datacontract.DataContractType;
import com.linkedin.datahub.graphql.types.form.FormType;
import com.linkedin.datahub.graphql.types.glossary.GlossaryNodeType;
import com.linkedin.datahub.graphql.types.glossary.GlossaryTermType;
import com.linkedin.datahub.graphql.types.monitor.MonitorType;
import com.linkedin.datahub.graphql.types.tag.TagType;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.config.ActionPipelineConfiguration;
import com.linkedin.metadata.config.ExecutorConfiguration;
import com.linkedin.metadata.connection.ConnectionService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.integration.IntegrationsService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.service.MonitorService;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.metadata.service.ShareService;
import com.linkedin.metadata.service.SubscriptionService;
import com.linkedin.metadata.test.TestEngine;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.usage.UsageClient;
import graphql.schema.idl.RuntimeWiring;
import io.datahubproject.metadata.services.SecretService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class AcrylGraphQLPlugin implements GmsGraphQLPlugin {

  // OSS Types
  private GlossaryTermType glossaryTermType;
  private GlossaryNodeType glossaryNodeType;
  private TagType tagType;
  private FormType formType;

  // Acryl Types
  private DataHubConnectionType connectionType; // Saas-ONLY
  private ActionPipelineType actionPipelineType; // Saas-ONLY
  private MonitorType monitorType; // SaaS only
  private AnomalyType anomalyType;
  private DataContractType dataContractType; // SaaS only, will be moved to OSS

  private List<EntityType<?, ?>> entityTypes;

  private EntityService entityService;
  private ConnectionService connectionService;
  private SecretService secretService;
  private IntegrationsService integrationsService;
  private ProposalService proposalService;
  private AssertionService assertionService;
  private EntitySearchService entitySearchService;
  private MonitorService monitorService;
  private SubscriptionService subscriptionService;
  private GroupService groupService;
  private SettingsService settingsService;
  private ShareService shareService;
  private FormService formService;
  private TimeseriesAspectService timeseriesAspectService;

  // Config
  private ExecutorConfiguration executorConfiguration;
  private ActionPipelineConfiguration actionConfiguration;

  // Clients
  private UsageClient usageClient;
  private EntityClient entityClient;
  private SystemEntityClient systemEntityClient;
  private GraphClient graphClient;
  private EntityRegistry entityRegistry;

  private TestEngine testEngine;
  private boolean initialized;
  private FeatureFlags featureFlags;

  public AcrylGraphQLPlugin() {
    this.initialized = false;
  }

  @Override
  public void init(GmsGraphQLEngineArgs args) {
    this.featureFlags = args.getFeatureFlags();
    this.graphClient = args.getGraphClient();
    this.entityClient = args.getEntityClient();
    this.systemEntityClient = args.getSystemEntityClient();
    this.entityService = args.getEntityService();
    this.entityRegistry = args.getEntityRegistry();
    this.usageClient = args.getUsageClient();
    this.connectionService = args.getConnectionService();
    this.assertionService = args.getAssertionService();
    this.secretService = args.getSecretService();
    this.monitorService = args.getMonitorService();
    this.proposalService = args.getProposalService();
    this.integrationsService = args.getIntegrationsService();
    this.entitySearchService = args.getEntitySearchService();
    this.subscriptionService = args.getSubscriptionService();
    this.groupService = args.getGroupService();
    this.settingsService = args.getSettingsService();
    this.timeseriesAspectService = args.getTimeseriesAspectService();
    this.testEngine = args.getTestEngine();
    this.shareService = args.getShareService();
    this.formService = args.getFormService();

    this.glossaryTermType = new GlossaryTermType(args.getEntityClient());
    this.glossaryNodeType = new GlossaryNodeType(args.getEntityClient());
    this.tagType = new TagType(args.getEntityClient());
    this.formType = new FormType(args.getEntityClient());

    this.connectionType =
        new DataHubConnectionType(args.getEntityClient(), args.getSecretService()); // SaaS only
    this.actionPipelineType = new ActionPipelineType(args.getEntityClient()); // SaaS only
    this.monitorType = new MonitorType(args.getEntityClient()); // SaaS only
    this.anomalyType = new AnomalyType(args.getEntityClient()); // SaaS only
    this.dataContractType = new DataContractType(entityClient); // SaaS only

    // New saas types
    this.entityTypes =
        ImmutableList.of(
            this.connectionType,
            this.actionPipelineType,
            this.monitorType,
            this.anomalyType,
            this.dataContractType);
    this.executorConfiguration = args.getExecutorConfiguration();
    this.actionConfiguration = args.getActionPipelineConfiguration();

    this.initialized = true;
  }

  @Override
  public List<String> getSchemaFiles() {
    return ImmutableList.of(
        ACTIONS_SCHEMA_FILE,
        ACTIONS_PIPELINE_SCHEMA_FILE,
        CONSTRAINTS_SCHEMA_FILE,
        ASSERTIONS_SCHEMA_FILE,
        CONNECTIONS_SCHEMA_FILE,
        MONITORS_SCHEMA_FILE,
        ANOMALY_SCHEMA_FILE,
        INTEGRATIONS_SCHEMA_FILE,
        NOTIFICATIONS_SCHEMA_FILE,
        SUBSCRIPTIONS_SCHEMA_FILE,
        CONTRACTS_SCHEMA_FILE,
        AI_SCHEMA_FILE,
        SHARE_SCHEMA_FILE,
        FORMS_ACRYL_SCHEMA_FILE,
        EXECUTOR_SCHEMA_FILE);
  }

  @Override
  public Collection<? extends LoadableType<?, ?>> getLoadableTypes() {
    assert initialized;
    return ImmutableList.of(
        connectionType, // Saas only
        actionPipelineType, // Saas only
        monitorType, // SaaS only
        anomalyType, // SaaS only
        dataContractType);
  }

  @Override
  public Collection<? extends EntityType<?, ?>> getEntityTypes() {
    return this.entityTypes;
  }

  @Override
  public void configureExtraResolvers(RuntimeWiring.Builder builder, GmsGraphQLEngine baseEngine) {
    configureGenericResolvers(builder, baseEngine);
    configureQueryResolvers(builder);
    configureMutationResolvers(builder, baseEngine);
    configureContainerResolvers(builder);
    configureDatasetResolvers(builder);
    configureActionRequestResolvers(builder);
    configureActionPipelineResolvers(builder, baseEngine);
    configureAnomalyResolvers(builder, baseEngine);
    configureConnectionResolvers(builder, baseEngine);
    configureIntegrationResolvers(builder);
    configureMonitorResolvers(builder, baseEngine);
    configureResolvedAuditStampResolvers(builder, baseEngine);
    configureGlobalSettingsResolvers(builder);
    configureTestResolvers(builder);
    configureProposalResolvers(builder);
    configureContractResolvers(builder, baseEngine);
    configureShareResolvers(builder, baseEngine);
    configureFormsForActorResolver(builder);
    configureExecutorResolvers(builder, baseEngine);
    configureFormAnalyticsResolver(builder, baseEngine);
  }

  private void configureMutationResolvers(
      final RuntimeWiring.Builder builder, GmsGraphQLEngine baseEngine) {
    builder.type(
        "Mutation",
        typeWiring ->
            typeWiring
                .dataFetcher(
                    "createTermConstraint", new CreateTermConstraintResolver(this.entityClient))
                .dataFetcher(
                    "acceptProposal", new AcceptProposalResolver(entityService, proposalService))
                .dataFetcher(
                    "rejectProposal", new RejectProposalResolver(entityService, proposalService))
                .dataFetcher("proposeTag", new ProposeTagResolver(entityService, entityClient))
                .dataFetcher("proposeTerm", new ProposeTermResolver(entityService, entityClient))
                .dataFetcher(
                    "proposeCreateGlossaryTerm",
                    new ProposeCreateGlossaryTermResolver(proposalService))
                .dataFetcher(
                    "proposeCreateGlossaryNode",
                    new ProposeCreateGlossaryNodeResolver(proposalService))
                .dataFetcher(
                    "proposeUpdateDescription",
                    new ProposeUpdateDescriptionResolver(proposalService))
                .dataFetcher(
                    "proposeDataContract", new ProposeDataContractResolver(proposalService))
                .dataFetcher(
                    "createDatasetAssertion", new CreateDatasetAssertionResolver(assertionService))
                .dataFetcher(
                    "createFreshnessAssertion",
                    new CreateFreshnessAssertionResolver(assertionService))
                .dataFetcher(
                    "upsertDatasetFreshnessAssertionMonitor",
                    new UpsertDatasetFreshnessAssertionMonitorResolver(
                        assertionService, monitorService, graphClient))
                .dataFetcher(
                    "upsertDatasetVolumeAssertionMonitor",
                    new UpsertDatasetVolumeAssertionMonitorResolver(
                        assertionService, monitorService, graphClient))
                .dataFetcher(
                    "upsertDatasetSqlAssertionMonitor",
                    new UpsertDatasetSqlAssertionMonitorResolver(
                        assertionService, monitorService, graphClient))
                .dataFetcher(
                    "upsertDatasetFieldAssertionMonitor",
                    new UpsertDatasetFieldAssertionMonitorResolver(
                        assertionService, monitorService, graphClient))
                .dataFetcher(
                    "createVolumeAssertion", new CreateVolumeAssertionResolver(assertionService))
                .dataFetcher(
                    "createFieldAssertion", new CreateFieldAssertionResolver(assertionService))
                .dataFetcher("createSqlAssertion", new CreateSqlAssertionResolver(assertionService))
                .dataFetcher("testAssertion", new TestAssertionResolver(monitorService))
                .dataFetcher(
                    "updateDatasetAssertion", new UpdateDatasetAssertionResolver(assertionService))
                .dataFetcher(
                    "updateAssertionMetadata",
                    new UpdateAssertionMetadataResolver(assertionService))
                .dataFetcher(
                    "createTest", new CreateTestResolver(this.entityClient, this.testEngine))
                .dataFetcher(
                    "updateTest", new UpdateTestResolver(this.entityClient, this.testEngine))
                .dataFetcher(
                    "deleteTest", new DeleteTestResolver(this.entityClient, this.testEngine))
                .dataFetcher("runTests", new RunTestsResolver(this.testEngine))
                .dataFetcher(
                    "batchAssignRole", new BatchAssignRoleResolver(baseEngine.getRoleService()))
                .dataFetcher(
                    "updateGroupNotificationSettings",
                    new UpdateGroupNotificationSettingsResolver(this.settingsService))
                .dataFetcher(
                    "updateUserNotificationSettings",
                    new UpdateUserNotificationSettingsResolver(this.settingsService))
                .dataFetcher(
                    "updateGroupNotificationSettings",
                    new UpdateGroupNotificationSettingsResolver(this.settingsService))
                .dataFetcher(
                    "createSubscription", new CreateSubscriptionResolver(this.subscriptionService))
                .dataFetcher(
                    "updateSubscription", new UpdateSubscriptionResolver(this.subscriptionService))
                .dataFetcher(
                    "deleteSubscription",
                    new DeleteSubscriptionResolver(this.subscriptionService, this.entityClient))
                .dataFetcher(
                    "suggestDescription", new SuggestDescriptionResolver(this.integrationsService))
                .dataFetcher(
                    "batchSubmitFormPrompt", new BatchSubmitFormPromptResolver(this.formService))
                .dataFetcher(
                    "batchVerifyForm",
                    new BatchVerifyFormResolver(this.formService, this.groupService))
                .dataFetcher("updateHelpLink", new UpdateHelpLinkResolver(this.settingsService))
                .dataFetcher(
                    "updateIncident",
                    new UpdateIncidentResolver(this.entityClient, this.entityService)));
  }

  private void configureQueryResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "Query",
        typeWiring ->
            typeWiring
                .dataFetcher("listActionRequests", new ListActionRequestsResolver(entityClient))
                .dataFetcher(
                    "listRejectedActionRequests",
                    new ListRejectedActionRequestsResolver(entityClient, entityService))
                .dataFetcher("validateTest", new ValidateTestResolver(testEngine))
                .dataFetcher(
                    "getUserNotificationSettings",
                    new GetUserNotificationSettingsResolver(this.settingsService))
                .dataFetcher(
                    "getGroupNotificationSettings",
                    new GetGroupNotificationSettingsResolver(this.settingsService))
                .dataFetcher(
                    "getSubscription", new GetSubscriptionResolver(this.subscriptionService))
                .dataFetcher(
                    "listSubscriptions", new ListSubscriptionsResolver(this.subscriptionService))
                .dataFetcher(
                    "getFormsForActor",
                    new GetFormsForActorResolver(this.groupService, this.formService))
                .dataFetcher(
                    "getEntitySubscriptionSummary",
                    new GetEntitySubscriptionSummaryResolver(
                        this.subscriptionService, this.groupService))
                .dataFetcher(
                    "formAnalyticsConfig",
                    new FormAnalyticsConfigResolver(this.integrationsService, this.featureFlags))
                .dataFetcher(
                    "formAnalytics",
                    new FormAnalyticsResolver(
                        this.entityClient, this.integrationsService, this.featureFlags)));
  }

  private void configureContainerResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "Container",
        typeWiring ->
            typeWiring
                // Proposals not in OSS
                .dataFetcher(
                "proposals",
                new ProposalsResolver((env) -> ((Entity) env.getSource()).getUrn(), entityClient)));
  }

  private void configureActionRequestResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "GlossaryTermProposalParams",
        typeWiring ->
            typeWiring.dataFetcher(
                "glossaryTerm",
                new LoadableTypeResolver<>(
                    glossaryTermType,
                    (env) ->
                        ((GlossaryTermProposalParams) env.getSource())
                            .getGlossaryTerm()
                            .getUrn())));
    builder.type(
        "TagProposalParams",
        typeWiring ->
            typeWiring.dataFetcher(
                "tag",
                new LoadableTypeResolver<>(
                    tagType, (env) -> ((TagProposalParams) env.getSource()).getTag().getUrn())));
    builder.type(
        "CreateGlossaryEntityProposalProperties",
        typeWiring ->
            typeWiring.dataFetcher(
                "parentNode",
                new LoadableTypeResolver<>(
                    glossaryNodeType,
                    (env) -> {
                      final CreateGlossaryEntityProposalProperties proposalProperties =
                          env.getSource();
                      return proposalProperties.getParentNode() != null
                          ? proposalProperties.getParentNode().getUrn()
                          : null;
                    })));
  }

  private void configureAnomalyResolvers(
      final RuntimeWiring.Builder builder, GmsGraphQLEngine baseEngine) {
    builder.type(
        "Anomaly",
        typeWiring ->
            typeWiring
                .dataFetcher(
                    "entity",
                    new EntityTypeResolver(
                        baseEngine.entityTypes, (env) -> ((Anomaly) env.getSource()).getEntity()))
                .dataFetcher(
                    "relationships", new EntityRelationshipsResultResolver(this.graphClient)));
    builder.type(
        "AnomalySource",
        typeWiring ->
            typeWiring.dataFetcher(
                "source",
                new LoadableTypeResolver<>(
                    baseEngine.getAssertionType(),
                    (env) -> {
                      final AnomalySource anomalySource = env.getSource();
                      return anomalySource.getSource() != null
                          ? anomalySource.getSource().getUrn()
                          : null;
                    })));
    builder.type(
        "EntityAnomaliesResult",
        typeWiring ->
            typeWiring.dataFetcher(
                "anomalies",
                new LoadableTypeBatchResolver<>(
                    anomalyType,
                    (env) ->
                        ((EntityAnomaliesResult) env.getSource())
                            .getAnomalies().stream()
                                .map(Anomaly::getUrn)
                                .collect(Collectors.toList()))));
    builder.type(
        "Dataset",
        typeWiring ->
            typeWiring.dataFetcher("anomalies", new EntityAnomaliesResolver(entityClient)));
    builder.type(
        "DataJob",
        typeWiring ->
            typeWiring.dataFetcher("anomalies", new EntityAnomaliesResolver(entityClient)));
  }

  private void configureConnectionResolvers(
      final RuntimeWiring.Builder builder, GmsGraphQLEngine baseEngine) {
    builder.type(
        "Mutation",
        typeWiring ->
            typeWiring.dataFetcher(
                "upsertConnection",
                new UpsertConnectionResolver(connectionService, secretService)));
    builder.type(
        "Query",
        typeWiring -> typeWiring.dataFetcher("connection", baseEngine.getResolver(connectionType)));
    builder.type(
        "DataHubConnection",
        typeWiring ->
            typeWiring.dataFetcher(
                "platform",
                new LoadableTypeResolver<>(
                    baseEngine.getDataPlatformType(),
                    (env) -> {
                      final DataHubConnection connection = env.getSource();
                      return connection.getPlatform() != null
                          ? connection.getPlatform().getUrn()
                          : null;
                    })));
  }

  private void configureActionPipelineResolvers(
      final RuntimeWiring.Builder builder, GmsGraphQLEngine baseEngine) {
    builder.type(
        "Mutation",
        typeWiring ->
            typeWiring
                .dataFetcher(
                    "createActionPipeline",
                    new UpsertActionPipelineResolver(this.entityClient, this.integrationsService))
                .dataFetcher(
                    "upsertActionPipeline",
                    new UpsertActionPipelineResolver(this.entityClient, this.integrationsService)));

    builder.type(
        "Query",
        typeWiring ->
            typeWiring
                .dataFetcher("actionPipeline", baseEngine.getResolver(actionPipelineType))
                .dataFetcher("listActionPipelines", new ListActionPipelineResolver(entityClient)));
  }

  private void configureShareResolvers(
      final RuntimeWiring.Builder builder, GmsGraphQLEngine baseEngine) {
    builder.type(
        "Mutation",
        typeWiring ->
            typeWiring
                .dataFetcher(
                    "shareEntity", new ShareEntityResolver(shareService, integrationsService))
                .dataFetcher(
                    "unshareEntity", new UnshareEntityResolver(shareService, integrationsService)));
    builder.type(
        "ShareResult",
        typeWiring ->
            typeWiring
                .dataFetcher(
                    "destination",
                    new LoadableTypeResolver<>(
                        connectionType,
                        (env) -> {
                          final ShareResult shareResult = env.getSource();
                          return shareResult.getDestination().getUrn();
                        }))
                .dataFetcher(
                    "implicitShareEntity",
                    new EntityTypeResolver(
                        baseEngine.entityTypes,
                        (env) -> {
                          final ShareResult shareResult = env.getSource();
                          return shareResult.getImplicitShareEntity() != null
                              ? shareResult.getImplicitShareEntity()
                              : null;
                        })));
  }

  private void configureIntegrationResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "Query",
        typeWiring ->
            typeWiring.dataFetcher(
                "getLinkPreview", new GetLinkPreviewResolver(this.integrationsService)));
  }

  private void configureMonitorResolvers(
      final RuntimeWiring.Builder builder, GmsGraphQLEngine baseEngine) {
    builder.type(
        "Mutation",
        typeWiring ->
            typeWiring
                .dataFetcher(
                    "deleteMonitor", new DeleteMonitorResolver(entityClient, entityService))
                .dataFetcher(
                    "createAssertionMonitor",
                    new CreateAssertionMonitorResolver(monitorService, assertionService))
                .dataFetcher(
                    "updateSystemMonitors",
                    new UpdateSystemMonitorsResolver(this.monitorService, this.entityClient))
                .dataFetcher(
                    "updateMonitorStatus",
                    new UpdateMonitorStatusResolver(this.monitorService, this.entityClient)));
    builder.type(
        "AssertionEvaluationSpec",
        typeWiring ->
            typeWiring.dataFetcher(
                "assertion",
                new LoadableTypeResolver<>(
                    baseEngine.getAssertionType(),
                    (env) -> {
                      final AssertionEvaluationSpec evaluationSpec = env.getSource();
                      return evaluationSpec.getAssertion() != null
                          ? evaluationSpec.getAssertion().getUrn()
                          : null;
                    })));
    builder.type(
        "Monitor",
        typeWiring ->
            typeWiring
                .dataFetcher(
                    "entity",
                    new EntityTypeResolver(
                        baseEngine.entityTypes, (env) -> ((Monitor) env.getSource()).getEntity()))
                .dataFetcher(
                    "aspects", new WeaklyTypedAspectsResolver(entityClient, entityRegistry)));
    builder.type(
        "Query",
        typeWiring ->
            typeWiring.dataFetcher(
                "systemMonitors",
                new SystemMonitorsResolver(this.monitorService, this.entityClient)));
    builder.type(
        "SystemMonitor",
        typeWiring ->
            typeWiring.dataFetcher(
                "monitor",
                new LoadableTypeResolver<>(
                    monitorType,
                    (env) -> {
                      final SystemMonitor monitor = env.getSource();
                      return monitor.getMonitor() != null ? monitor.getMonitor().getUrn() : null;
                    })));
  }

  private void configureResolvedAuditStampResolvers(
      final RuntimeWiring.Builder builder, final GmsGraphQLEngine baseEngine) {
    builder.type(
        "ResolvedAuditStamp",
        typeWiring ->
            typeWiring.dataFetcher(
                "actor",
                new LoadableTypeResolver<>(
                    baseEngine.getCorpUserType(),
                    (env) -> ((ResolvedAuditStamp) env.getSource()).getActor().getUrn())));
  }

  private void configureGlobalSettingsResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "Query",
        typeWiring ->
            typeWiring.dataFetcher(
                "globalSettings", new GlobalSettingsResolver(entityClient, secretService)));
    builder.type(
        "Mutation",
        typeWiring ->
            typeWiring.dataFetcher(
                "updateGlobalSettings",
                new UpdateGlobalSettingsResolver(entityClient, secretService)));
  }

  private void configureTestResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "Mutation",
        typeWiring ->
            typeWiring.dataFetcher("runTestDefinition", new RunTestDefinitionResolver(testEngine)));
    builder.type(
        "Test",
        typeWiring ->
            typeWiring
                .dataFetcher(
                    "results",
                    new TestResultsSummaryResolver(
                        this.entitySearchService, this.entityService, this.timeseriesAspectService))
                .dataFetcher("batchRunEvents", new BatchTestRunEventsResolver(this.entityClient)));
  }

  private void configureDatasetResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "Dataset",
        typeWiring ->
            typeWiring
                .dataFetcher(
                    "statsSummary",
                    new DatasetStatsSummaryResolver(this.systemEntityClient, this.usageClient))
                .dataFetcher("testResults", new EntityTestResultsResolver(entityClient))
                .dataFetcher(
                    "constraints",
                    new ConstraintsResolver(
                        (env) -> ((Entity) env.getSource()).getUrn(), entityService, entityClient))
                .dataFetcher(
                    "proposals",
                    new ProposalsResolver(
                        (env) -> ((Entity) env.getSource()).getUrn(), entityClient)));
  }

  private void configureProposalResolvers(final RuntimeWiring.Builder builder) {
    List<String> entitiesWithProposal =
        ImmutableList.of(
            "Notebook",
            "Dashboard",
            "DataJob",
            "DataFlow",
            "MLFeatureTable",
            "MLModel",
            "MLModelGroup",
            "MLFeature",
            "MLPrimaryKey");

    for (String entity : entitiesWithProposal) {
      builder.type(
          entity,
          typeWiring ->
              typeWiring.dataFetcher(
                  "proposals",
                  new ProposalsResolver(
                      (env) -> ((Entity) env.getSource()).getUrn(), entityClient)));
    }
  }

  private void configureGenericResolvers(
      RuntimeWiring.Builder builder, GmsGraphQLEngine baseEngine) {
    builder
        .type(
            "GlossaryTermAssociation",
            typeWiring ->
                typeWiring.dataFetcher(
                    "actor",
                    new LoadableTypeResolver<>(
                        baseEngine.getCorpUserType(),
                        (env) -> {
                          final GlossaryTermAssociation association = env.getSource();
                          return association.getActor() != null
                              ? association.getActor().getUrn()
                              : null;
                        })))
        .type(
            "ActionRequest",
            typeWiring ->
                typeWiring.dataFetcher(
                    "entity",
                    new EntityTypeResolver(
                        new ArrayList<>(baseEngine.entityTypes),
                        (env) -> ((ActionRequest) env.getSource()).getEntity())))
        .type(
            "DataHubSubscription",
            typeWiring ->
                typeWiring.dataFetcher(
                    "entity",
                    new EntityTypeResolver(
                        baseEngine.entityTypes,
                        (env) -> ((DataHubSubscription) env.getSource()).getEntity())))
        .type(
            "EntitySubscriptionSummary",
            typeWiring ->
                typeWiring.dataFetcher(
                    "exampleGroups",
                    new EntityTypeBatchResolver(
                        baseEngine.entityTypes,
                        (env) ->
                            ((EntitySubscriptionSummary) env.getSource())
                                .getExampleGroups().stream()
                                    .map(group -> (Entity) group)
                                    .collect(Collectors.toList()))));
    builder.type(
        "ChartStatsSummary",
        typeWiring ->
            typeWiring.dataFetcher(
                "topUsersLast30Days",
                new LoadableTypeBatchResolver<>(
                    baseEngine.getCorpUserType(),
                    (env) -> {
                      ChartStatsSummary summary = ((ChartStatsSummary) env.getSource());
                      return summary.getTopUsersLast30Days() != null
                          ? summary.getTopUsersLast30Days().stream()
                              .map(CorpUser::getUrn)
                              .collect(Collectors.toList())
                          : null;
                    })));
    builder.type(
        "DataJob",
        typeWiring ->
            typeWiring.dataFetcher(
                "assertions", new EntityAssertionsResolver(entityClient, graphClient)));
  }

  private void configureContractResolvers(
      final RuntimeWiring.Builder builder, final GmsGraphQLEngine baseEngine) {
    builder.type(
        "Dataset",
        typeWiring ->
            typeWiring.dataFetcher(
                "contract", new EntityDataContractResolver(this.entityClient, this.graphClient)));
    builder.type(
        "FreshnessContract",
        typeWiring ->
            typeWiring.dataFetcher(
                "assertion",
                new LoadableTypeResolver<>(
                    baseEngine.getAssertionType(),
                    (env) -> {
                      final FreshnessContract contract = env.getSource();
                      return contract.getAssertion() != null
                          ? contract.getAssertion().getUrn()
                          : null;
                    })));
    builder.type(
        "DataQualityContract",
        typeWiring ->
            typeWiring.dataFetcher(
                "assertion",
                new LoadableTypeResolver<>(
                    baseEngine.getAssertionType(),
                    (env) -> {
                      final DataQualityContract contract = env.getSource();
                      return contract.getAssertion() != null
                          ? contract.getAssertion().getUrn()
                          : null;
                    })));
    builder.type(
        "SchemaContract",
        typeWiring ->
            typeWiring.dataFetcher(
                "assertion",
                new LoadableTypeResolver<>(
                    baseEngine.getAssertionType(),
                    (env) -> {
                      final SchemaContract contract = env.getSource();
                      return contract.getAssertion() != null
                          ? contract.getAssertion().getUrn()
                          : null;
                    })));
    builder.type(
        "Mutation",
        typeWiring ->
            typeWiring.dataFetcher(
                "upsertDataContract",
                new UpsertDataContractResolver(this.entityClient, this.graphClient)));
  }

  private void configureFormsForActorResolver(final RuntimeWiring.Builder builder) {
    builder.type(
        "FormForActor",
        typeWiring ->
            typeWiring
                .dataFetcher(
                    "form",
                    new LoadableTypeResolver<>(
                        formType, (env) -> ((FormForActor) env.getSource()).getForm().getUrn()))
                .dataFetcher(
                    "numEntitiesToComplete",
                    new NumEntitiesToCompleteResolver(this.entityClient, this.formService)));
  }

  private void configureExecutorResolvers(
      final RuntimeWiring.Builder builder, final GmsGraphQLEngine baseEngine) {
    builder.type(
        "Query",
        typeWiring ->
            typeWiring
                .dataFetcher(
                    "listSignalRequests", new ListSignalRequestsResolver(this.entityClient))
                .dataFetcher(
                    "listExecutorConfigs",
                    new ListExecutorConfigsResolver(
                        this.entityClient, this.executorConfiguration)));
  }

  private void configureFormAnalyticsResolver(
      final RuntimeWiring.Builder builder, final GmsGraphQLEngine baseEngine) {
    builder.type(
        "RowResult",
        typeWiring ->
            typeWiring.dataFetcher(
                "entity",
                new EntityTypeResolver(
                    baseEngine.entityTypes, (env) -> ((RowResult) env.getSource()).getEntity())));
  }
}
