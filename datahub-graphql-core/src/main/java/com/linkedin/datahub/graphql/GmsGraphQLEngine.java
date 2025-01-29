package com.linkedin.datahub.graphql;

import static com.linkedin.datahub.graphql.Constants.*;
import static com.linkedin.metadata.Constants.*;
import static graphql.scalars.ExtendedScalars.*;

import com.datahub.authentication.AuthenticationConfiguration;
import com.datahub.authentication.group.GroupService;
import com.datahub.authentication.invite.InviteTokenService;
import com.datahub.authentication.post.PostService;
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
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.AccessToken;
import com.linkedin.datahub.graphql.generated.AccessTokenMetadata;
import com.linkedin.datahub.graphql.generated.ActorFilter;
import com.linkedin.datahub.graphql.generated.AggregationMetadata;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.AutoCompleteResultForEntity;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.BrowsePathEntry;
import com.linkedin.datahub.graphql.generated.BrowseResultGroupV2;
import com.linkedin.datahub.graphql.generated.BrowseResults;
import com.linkedin.datahub.graphql.generated.BusinessAttribute;
import com.linkedin.datahub.graphql.generated.BusinessAttributeAssociation;
import com.linkedin.datahub.graphql.generated.Chart;
import com.linkedin.datahub.graphql.generated.ChartInfo;
import com.linkedin.datahub.graphql.generated.Container;
import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.CorpGroupInfo;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.CorpUserEditableProperties;
import com.linkedin.datahub.graphql.generated.CorpUserInfo;
import com.linkedin.datahub.graphql.generated.CorpUserViewsSettings;
import com.linkedin.datahub.graphql.generated.Dashboard;
import com.linkedin.datahub.graphql.generated.DashboardInfo;
import com.linkedin.datahub.graphql.generated.DashboardStatsSummary;
import com.linkedin.datahub.graphql.generated.DashboardUserUsageCounts;
import com.linkedin.datahub.graphql.generated.DataFlow;
import com.linkedin.datahub.graphql.generated.DataHubConnection;
import com.linkedin.datahub.graphql.generated.DataHubView;
import com.linkedin.datahub.graphql.generated.DataJob;
import com.linkedin.datahub.graphql.generated.DataJobInputOutput;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.DataPlatformInstance;
import com.linkedin.datahub.graphql.generated.DataProcessInstance;
import com.linkedin.datahub.graphql.generated.DataQualityContract;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.DatasetStatsSummary;
import com.linkedin.datahub.graphql.generated.Deprecation;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.ERModelRelationship;
import com.linkedin.datahub.graphql.generated.ERModelRelationshipProperties;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityPath;
import com.linkedin.datahub.graphql.generated.EntityRelationship;
import com.linkedin.datahub.graphql.generated.EntityRelationshipLegacy;
import com.linkedin.datahub.graphql.generated.FacetMetadata;
import com.linkedin.datahub.graphql.generated.ForeignKeyConstraint;
import com.linkedin.datahub.graphql.generated.FormActorAssignment;
import com.linkedin.datahub.graphql.generated.FreshnessContract;
import com.linkedin.datahub.graphql.generated.GetRootGlossaryNodesResult;
import com.linkedin.datahub.graphql.generated.GetRootGlossaryTermsResult;
import com.linkedin.datahub.graphql.generated.GlossaryNode;
import com.linkedin.datahub.graphql.generated.GlossaryTerm;
import com.linkedin.datahub.graphql.generated.GlossaryTermAssociation;
import com.linkedin.datahub.graphql.generated.IncidentSource;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.datahub.graphql.generated.InstitutionalMemoryMetadata;
import com.linkedin.datahub.graphql.generated.LineageRelationship;
import com.linkedin.datahub.graphql.generated.ListAccessTokenResult;
import com.linkedin.datahub.graphql.generated.ListBusinessAttributesResult;
import com.linkedin.datahub.graphql.generated.ListDomainsResult;
import com.linkedin.datahub.graphql.generated.ListGroupsResult;
import com.linkedin.datahub.graphql.generated.ListOwnershipTypesResult;
import com.linkedin.datahub.graphql.generated.ListQueriesResult;
import com.linkedin.datahub.graphql.generated.ListTestsResult;
import com.linkedin.datahub.graphql.generated.ListViewsResult;
import com.linkedin.datahub.graphql.generated.MLFeature;
import com.linkedin.datahub.graphql.generated.MLFeatureProperties;
import com.linkedin.datahub.graphql.generated.MLFeatureTable;
import com.linkedin.datahub.graphql.generated.MLFeatureTableProperties;
import com.linkedin.datahub.graphql.generated.MLModel;
import com.linkedin.datahub.graphql.generated.MLModelGroup;
import com.linkedin.datahub.graphql.generated.MLModelProperties;
import com.linkedin.datahub.graphql.generated.MLPrimaryKey;
import com.linkedin.datahub.graphql.generated.MLPrimaryKeyProperties;
import com.linkedin.datahub.graphql.generated.MatchedField;
import com.linkedin.datahub.graphql.generated.MetadataAttribution;
import com.linkedin.datahub.graphql.generated.Notebook;
import com.linkedin.datahub.graphql.generated.Owner;
import com.linkedin.datahub.graphql.generated.OwnershipTypeEntity;
import com.linkedin.datahub.graphql.generated.ParentDomainsResult;
import com.linkedin.datahub.graphql.generated.PolicyMatchCriterionValue;
import com.linkedin.datahub.graphql.generated.QueryEntity;
import com.linkedin.datahub.graphql.generated.QueryProperties;
import com.linkedin.datahub.graphql.generated.QuerySubject;
import com.linkedin.datahub.graphql.generated.QuickFilter;
import com.linkedin.datahub.graphql.generated.RecommendationContent;
import com.linkedin.datahub.graphql.generated.ResolvedAuditStamp;
import com.linkedin.datahub.graphql.generated.SchemaContract;
import com.linkedin.datahub.graphql.generated.SchemaField;
import com.linkedin.datahub.graphql.generated.SchemaFieldEntity;
import com.linkedin.datahub.graphql.generated.SearchAcrossLineageResult;
import com.linkedin.datahub.graphql.generated.SearchResult;
import com.linkedin.datahub.graphql.generated.SiblingProperties;
import com.linkedin.datahub.graphql.generated.StructuredPropertiesEntry;
import com.linkedin.datahub.graphql.generated.StructuredPropertyDefinition;
import com.linkedin.datahub.graphql.generated.StructuredPropertyParams;
import com.linkedin.datahub.graphql.generated.Test;
import com.linkedin.datahub.graphql.generated.TestResult;
import com.linkedin.datahub.graphql.generated.TypeQualifier;
import com.linkedin.datahub.graphql.generated.UserUsageCounts;
import com.linkedin.datahub.graphql.generated.VersionProperties;
import com.linkedin.datahub.graphql.generated.VersionSet;
import com.linkedin.datahub.graphql.resolvers.MeResolver;
import com.linkedin.datahub.graphql.resolvers.assertion.AssertionRunEventResolver;
import com.linkedin.datahub.graphql.resolvers.assertion.DeleteAssertionResolver;
import com.linkedin.datahub.graphql.resolvers.assertion.EntityAssertionsResolver;
import com.linkedin.datahub.graphql.resolvers.assertion.ReportAssertionResultResolver;
import com.linkedin.datahub.graphql.resolvers.assertion.UpsertCustomAssertionResolver;
import com.linkedin.datahub.graphql.resolvers.auth.CreateAccessTokenResolver;
import com.linkedin.datahub.graphql.resolvers.auth.DebugAccessResolver;
import com.linkedin.datahub.graphql.resolvers.auth.GetAccessTokenMetadataResolver;
import com.linkedin.datahub.graphql.resolvers.auth.GetAccessTokenResolver;
import com.linkedin.datahub.graphql.resolvers.auth.ListAccessTokensResolver;
import com.linkedin.datahub.graphql.resolvers.auth.RevokeAccessTokenResolver;
import com.linkedin.datahub.graphql.resolvers.browse.BrowsePathsResolver;
import com.linkedin.datahub.graphql.resolvers.browse.BrowseResolver;
import com.linkedin.datahub.graphql.resolvers.browse.EntityBrowsePathsResolver;
import com.linkedin.datahub.graphql.resolvers.businessattribute.AddBusinessAttributeResolver;
import com.linkedin.datahub.graphql.resolvers.businessattribute.CreateBusinessAttributeResolver;
import com.linkedin.datahub.graphql.resolvers.businessattribute.DeleteBusinessAttributeResolver;
import com.linkedin.datahub.graphql.resolvers.businessattribute.ListBusinessAttributesResolver;
import com.linkedin.datahub.graphql.resolvers.businessattribute.RemoveBusinessAttributeResolver;
import com.linkedin.datahub.graphql.resolvers.businessattribute.UpdateBusinessAttributeResolver;
import com.linkedin.datahub.graphql.resolvers.chart.BrowseV2Resolver;
import com.linkedin.datahub.graphql.resolvers.chart.ChartStatsSummaryResolver;
import com.linkedin.datahub.graphql.resolvers.config.AppConfigResolver;
import com.linkedin.datahub.graphql.resolvers.connection.UpsertConnectionResolver;
import com.linkedin.datahub.graphql.resolvers.container.ContainerEntitiesResolver;
import com.linkedin.datahub.graphql.resolvers.container.ParentContainersResolver;
import com.linkedin.datahub.graphql.resolvers.dashboard.DashboardStatsSummaryResolver;
import com.linkedin.datahub.graphql.resolvers.dashboard.DashboardUsageStatsResolver;
import com.linkedin.datahub.graphql.resolvers.datacontract.EntityDataContractResolver;
import com.linkedin.datahub.graphql.resolvers.datacontract.UpsertDataContractResolver;
import com.linkedin.datahub.graphql.resolvers.dataproduct.BatchSetDataProductResolver;
import com.linkedin.datahub.graphql.resolvers.dataproduct.CreateDataProductResolver;
import com.linkedin.datahub.graphql.resolvers.dataproduct.DeleteDataProductResolver;
import com.linkedin.datahub.graphql.resolvers.dataproduct.ListDataProductAssetsResolver;
import com.linkedin.datahub.graphql.resolvers.dataproduct.UpdateDataProductResolver;
import com.linkedin.datahub.graphql.resolvers.dataset.DatasetStatsSummaryResolver;
import com.linkedin.datahub.graphql.resolvers.dataset.DatasetUsageStatsResolver;
import com.linkedin.datahub.graphql.resolvers.dataset.IsAssignedToMeResolver;
import com.linkedin.datahub.graphql.resolvers.deprecation.UpdateDeprecationResolver;
import com.linkedin.datahub.graphql.resolvers.domain.CreateDomainResolver;
import com.linkedin.datahub.graphql.resolvers.domain.DeleteDomainResolver;
import com.linkedin.datahub.graphql.resolvers.domain.DomainEntitiesResolver;
import com.linkedin.datahub.graphql.resolvers.domain.ListDomainsResolver;
import com.linkedin.datahub.graphql.resolvers.domain.ParentDomainsResolver;
import com.linkedin.datahub.graphql.resolvers.domain.SetDomainResolver;
import com.linkedin.datahub.graphql.resolvers.domain.UnsetDomainResolver;
import com.linkedin.datahub.graphql.resolvers.embed.UpdateEmbedResolver;
import com.linkedin.datahub.graphql.resolvers.entity.EntityExistsResolver;
import com.linkedin.datahub.graphql.resolvers.entity.EntityPrivilegesResolver;
import com.linkedin.datahub.graphql.resolvers.entity.versioning.LinkAssetVersionResolver;
import com.linkedin.datahub.graphql.resolvers.entity.versioning.UnlinkAssetVersionResolver;
import com.linkedin.datahub.graphql.resolvers.form.BatchAssignFormResolver;
import com.linkedin.datahub.graphql.resolvers.form.BatchRemoveFormResolver;
import com.linkedin.datahub.graphql.resolvers.form.CreateDynamicFormAssignmentResolver;
import com.linkedin.datahub.graphql.resolvers.form.CreateFormResolver;
import com.linkedin.datahub.graphql.resolvers.form.DeleteFormResolver;
import com.linkedin.datahub.graphql.resolvers.form.IsFormAssignedToMeResolver;
import com.linkedin.datahub.graphql.resolvers.form.SubmitFormPromptResolver;
import com.linkedin.datahub.graphql.resolvers.form.UpdateFormResolver;
import com.linkedin.datahub.graphql.resolvers.form.VerifyFormResolver;
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
import com.linkedin.datahub.graphql.resolvers.health.EntityHealthResolver;
import com.linkedin.datahub.graphql.resolvers.incident.EntityIncidentsResolver;
import com.linkedin.datahub.graphql.resolvers.incident.RaiseIncidentResolver;
import com.linkedin.datahub.graphql.resolvers.incident.UpdateIncidentStatusResolver;
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
import com.linkedin.datahub.graphql.resolvers.ingest.secret.UpdateSecretResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.source.DeleteIngestionSourceResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.source.GetIngestionSourceResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.source.ListIngestionSourcesResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.source.UpsertIngestionSourceResolver;
import com.linkedin.datahub.graphql.resolvers.jobs.DataJobRunsResolver;
import com.linkedin.datahub.graphql.resolvers.jobs.EntityRunsResolver;
import com.linkedin.datahub.graphql.resolvers.lineage.UpdateLineageResolver;
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
import com.linkedin.datahub.graphql.resolvers.mutate.MoveDomainResolver;
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
import com.linkedin.datahub.graphql.resolvers.ownership.CreateOwnershipTypeResolver;
import com.linkedin.datahub.graphql.resolvers.ownership.DeleteOwnershipTypeResolver;
import com.linkedin.datahub.graphql.resolvers.ownership.ListOwnershipTypesResolver;
import com.linkedin.datahub.graphql.resolvers.ownership.UpdateOwnershipTypeResolver;
import com.linkedin.datahub.graphql.resolvers.policy.DeletePolicyResolver;
import com.linkedin.datahub.graphql.resolvers.policy.GetGrantedPrivilegesResolver;
import com.linkedin.datahub.graphql.resolvers.policy.ListPoliciesResolver;
import com.linkedin.datahub.graphql.resolvers.policy.UpsertPolicyResolver;
import com.linkedin.datahub.graphql.resolvers.post.CreatePostResolver;
import com.linkedin.datahub.graphql.resolvers.post.DeletePostResolver;
import com.linkedin.datahub.graphql.resolvers.post.ListPostsResolver;
import com.linkedin.datahub.graphql.resolvers.post.UpdatePostResolver;
import com.linkedin.datahub.graphql.resolvers.query.CreateQueryResolver;
import com.linkedin.datahub.graphql.resolvers.query.DeleteQueryResolver;
import com.linkedin.datahub.graphql.resolvers.query.ListQueriesResolver;
import com.linkedin.datahub.graphql.resolvers.query.UpdateQueryResolver;
import com.linkedin.datahub.graphql.resolvers.recommendation.ListRecommendationsResolver;
import com.linkedin.datahub.graphql.resolvers.role.AcceptRoleResolver;
import com.linkedin.datahub.graphql.resolvers.role.BatchAssignRoleResolver;
import com.linkedin.datahub.graphql.resolvers.role.CreateInviteTokenResolver;
import com.linkedin.datahub.graphql.resolvers.role.GetInviteTokenResolver;
import com.linkedin.datahub.graphql.resolvers.role.ListRolesResolver;
import com.linkedin.datahub.graphql.resolvers.search.AggregateAcrossEntitiesResolver;
import com.linkedin.datahub.graphql.resolvers.search.AutoCompleteForMultipleResolver;
import com.linkedin.datahub.graphql.resolvers.search.AutoCompleteResolver;
import com.linkedin.datahub.graphql.resolvers.search.GetQuickFiltersResolver;
import com.linkedin.datahub.graphql.resolvers.search.ScrollAcrossEntitiesResolver;
import com.linkedin.datahub.graphql.resolvers.search.ScrollAcrossLineageResolver;
import com.linkedin.datahub.graphql.resolvers.search.SearchAcrossEntitiesResolver;
import com.linkedin.datahub.graphql.resolvers.search.SearchAcrossLineageResolver;
import com.linkedin.datahub.graphql.resolvers.search.SearchResolver;
import com.linkedin.datahub.graphql.resolvers.settings.docPropagation.DocPropagationSettingsResolver;
import com.linkedin.datahub.graphql.resolvers.settings.docPropagation.UpdateDocPropagationSettingsResolver;
import com.linkedin.datahub.graphql.resolvers.settings.user.UpdateCorpUserViewsSettingsResolver;
import com.linkedin.datahub.graphql.resolvers.settings.view.GlobalViewsSettingsResolver;
import com.linkedin.datahub.graphql.resolvers.settings.view.UpdateGlobalViewsSettingsResolver;
import com.linkedin.datahub.graphql.resolvers.step.BatchGetStepStatesResolver;
import com.linkedin.datahub.graphql.resolvers.step.BatchUpdateStepStatesResolver;
import com.linkedin.datahub.graphql.resolvers.structuredproperties.CreateStructuredPropertyResolver;
import com.linkedin.datahub.graphql.resolvers.structuredproperties.DeleteStructuredPropertyResolver;
import com.linkedin.datahub.graphql.resolvers.structuredproperties.RemoveStructuredPropertiesResolver;
import com.linkedin.datahub.graphql.resolvers.structuredproperties.UpdateStructuredPropertyResolver;
import com.linkedin.datahub.graphql.resolvers.structuredproperties.UpsertStructuredPropertiesResolver;
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
import com.linkedin.datahub.graphql.resolvers.type.PropertyValueResolver;
import com.linkedin.datahub.graphql.resolvers.type.ResolvedActorResolver;
import com.linkedin.datahub.graphql.resolvers.type.ResultsTypeResolver;
import com.linkedin.datahub.graphql.resolvers.type.TimeSeriesAspectInterfaceTypeResolver;
import com.linkedin.datahub.graphql.resolvers.user.CreateNativeUserResetTokenResolver;
import com.linkedin.datahub.graphql.resolvers.user.ListUsersResolver;
import com.linkedin.datahub.graphql.resolvers.user.RemoveUserResolver;
import com.linkedin.datahub.graphql.resolvers.user.UpdateUserStatusResolver;
import com.linkedin.datahub.graphql.resolvers.versioning.VersionsSearchResolver;
import com.linkedin.datahub.graphql.resolvers.view.CreateViewResolver;
import com.linkedin.datahub.graphql.resolvers.view.DeleteViewResolver;
import com.linkedin.datahub.graphql.resolvers.view.ListGlobalViewsResolver;
import com.linkedin.datahub.graphql.resolvers.view.ListMyViewsResolver;
import com.linkedin.datahub.graphql.resolvers.view.UpdateViewResolver;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.EntityType;
import com.linkedin.datahub.graphql.types.LoadableType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.aspect.AspectType;
import com.linkedin.datahub.graphql.types.assertion.AssertionType;
import com.linkedin.datahub.graphql.types.auth.AccessTokenMetadataType;
import com.linkedin.datahub.graphql.types.businessattribute.BusinessAttributeType;
import com.linkedin.datahub.graphql.types.chart.ChartType;
import com.linkedin.datahub.graphql.types.common.mappers.OperationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.types.connection.DataHubConnectionType;
import com.linkedin.datahub.graphql.types.container.ContainerType;
import com.linkedin.datahub.graphql.types.corpgroup.CorpGroupType;
import com.linkedin.datahub.graphql.types.corpuser.CorpUserType;
import com.linkedin.datahub.graphql.types.dashboard.DashboardType;
import com.linkedin.datahub.graphql.types.dataflow.DataFlowType;
import com.linkedin.datahub.graphql.types.datajob.DataJobType;
import com.linkedin.datahub.graphql.types.dataplatform.DataPlatformType;
import com.linkedin.datahub.graphql.types.dataplatforminstance.DataPlatformInstanceType;
import com.linkedin.datahub.graphql.types.dataprocessinst.DataProcessInstanceType;
import com.linkedin.datahub.graphql.types.dataprocessinst.mappers.DataProcessInstanceRunEventMapper;
import com.linkedin.datahub.graphql.types.dataproduct.DataProductType;
import com.linkedin.datahub.graphql.types.dataset.DatasetType;
import com.linkedin.datahub.graphql.types.dataset.VersionedDatasetType;
import com.linkedin.datahub.graphql.types.dataset.mappers.DatasetProfileMapper;
import com.linkedin.datahub.graphql.types.datatype.DataTypeType;
import com.linkedin.datahub.graphql.types.domain.DomainType;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeType;
import com.linkedin.datahub.graphql.types.ermodelrelationship.CreateERModelRelationshipResolver;
import com.linkedin.datahub.graphql.types.ermodelrelationship.ERModelRelationshipType;
import com.linkedin.datahub.graphql.types.ermodelrelationship.UpdateERModelRelationshipResolver;
import com.linkedin.datahub.graphql.types.form.FormType;
import com.linkedin.datahub.graphql.types.glossary.GlossaryNodeType;
import com.linkedin.datahub.graphql.types.glossary.GlossaryTermType;
import com.linkedin.datahub.graphql.types.incident.IncidentType;
import com.linkedin.datahub.graphql.types.mlmodel.MLFeatureTableType;
import com.linkedin.datahub.graphql.types.mlmodel.MLFeatureType;
import com.linkedin.datahub.graphql.types.mlmodel.MLModelGroupType;
import com.linkedin.datahub.graphql.types.mlmodel.MLModelType;
import com.linkedin.datahub.graphql.types.mlmodel.MLPrimaryKeyType;
import com.linkedin.datahub.graphql.types.notebook.NotebookType;
import com.linkedin.datahub.graphql.types.ownership.OwnershipType;
import com.linkedin.datahub.graphql.types.policy.DataHubPolicyType;
import com.linkedin.datahub.graphql.types.query.QueryType;
import com.linkedin.datahub.graphql.types.restricted.RestrictedType;
import com.linkedin.datahub.graphql.types.role.DataHubRoleType;
import com.linkedin.datahub.graphql.types.rolemetadata.RoleType;
import com.linkedin.datahub.graphql.types.schemafield.SchemaFieldType;
import com.linkedin.datahub.graphql.types.structuredproperty.StructuredPropertyType;
import com.linkedin.datahub.graphql.types.tag.TagType;
import com.linkedin.datahub.graphql.types.test.TestType;
import com.linkedin.datahub.graphql.types.versioning.VersionSetType;
import com.linkedin.datahub.graphql.types.view.DataHubViewType;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.client.UsageStatsJavaClient;
import com.linkedin.metadata.config.DataHubConfiguration;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.metadata.config.TestsConfiguration;
import com.linkedin.metadata.config.ViewsConfiguration;
import com.linkedin.metadata.config.VisualConfiguration;
import com.linkedin.metadata.config.telemetry.TelemetryConfiguration;
import com.linkedin.metadata.connection.ConnectionService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.versioning.EntityVersioningService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.graph.SiblingGraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.recommendation.RecommendationsService;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.BusinessAttributeService;
import com.linkedin.metadata.service.DataProductService;
import com.linkedin.metadata.service.ERModelRelationshipService;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.service.LineageService;
import com.linkedin.metadata.service.OwnershipTypeService;
import com.linkedin.metadata.service.QueryService;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.version.GitVersion;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.StaticDataFetcher;
import graphql.schema.idl.RuntimeWiring;
import io.datahubproject.metadata.services.RestrictedService;
import io.datahubproject.metadata.services.SecretService;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.dataloader.BatchLoaderContextProvider;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderOptions;

/**
 * A {@link GraphQLEngine} configured to provide access to the entities and aspects on the GMS
 * graph.
 */
@Slf4j
@Getter
public class GmsGraphQLEngine {

  private final EntityClient entityClient;
  private final SystemEntityClient systemEntityClient;
  private final GraphClient graphClient;
  private final UsageStatsJavaClient usageClient;
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
  private final PostService postService;
  private final SettingsService settingsService;
  private final ViewService viewService;
  private final OwnershipTypeService ownershipTypeService;
  private final LineageService lineageService;
  private final QueryService queryService;
  private final DataProductService dataProductService;
  private final ERModelRelationshipService erModelRelationshipService;
  private final FormService formService;
  private final RestrictedService restrictedService;
  private ConnectionService connectionService;
  private AssertionService assertionService;
  private final EntityVersioningService entityVersioningService;

  private final BusinessAttributeService businessAttributeService;
  private final FeatureFlags featureFlags;

  private final IngestionConfiguration ingestionConfiguration;
  private final AuthenticationConfiguration authenticationConfiguration;
  private final AuthorizationConfiguration authorizationConfiguration;
  private final VisualConfiguration visualConfiguration;
  private final TelemetryConfiguration telemetryConfiguration;
  private final TestsConfiguration testsConfiguration;
  private final DataHubConfiguration datahubConfiguration;
  private final ViewsConfiguration viewsConfiguration;

  private final DatasetType datasetType;

  private final RoleType roleType;

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
  private final DataHubConnectionType connectionType;
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
  private final SchemaFieldType schemaFieldType;
  private final ERModelRelationshipType erModelRelationshipType;
  private final DataHubViewType dataHubViewType;
  private final QueryType queryType;
  private final DataProductType dataProductType;
  private final OwnershipType ownershipType;
  private final StructuredPropertyType structuredPropertyType;
  private final DataTypeType dataTypeType;
  private final EntityTypeType entityTypeType;
  private final FormType formType;
  private final IncidentType incidentType;
  private final RestrictedType restrictedType;
  private final DataProcessInstanceType dataProcessInstanceType;
  private final VersionSetType versionSetType;

  private final int graphQLQueryComplexityLimit;
  private final int graphQLQueryDepthLimit;
  private final boolean graphQLQueryIntrospectionEnabled;

  private final BusinessAttributeType businessAttributeType;

  /** A list of GraphQL Plugins that extend the core engine */
  private final List<GmsGraphQLPlugin> graphQLPlugins;

  /** Configures the graph objects that can be fetched primary key. */
  public final List<EntityType<?, ?>> entityTypes;

  /** Configures all graph objects */
  public final List<LoadableType<?, ?>> loadableTypes;

  /** Configures the graph objects for owner */
  public final List<LoadableType<?, ?>> ownerTypes;

  /** Configures the graph objects that can be searched. */
  public final List<SearchableEntityType<?, ?>> searchableTypes;

  /** Configures the graph objects that can be browsed. */
  public final List<BrowsableEntityType<?, ?>> browsableTypes;

  public GmsGraphQLEngine(final GmsGraphQLEngineArgs args) {

    this.graphQLPlugins =
        List.of(
            // Add new plugins here
            );

    this.graphQLPlugins.forEach(plugin -> plugin.init(args));

    this.entityClient = args.entityClient;
    this.systemEntityClient = args.systemEntityClient;
    this.graphClient = args.graphClient;
    this.usageClient = args.usageClient;
    this.siblingGraphService = args.siblingGraphService;

    this.analyticsService = args.analyticsService;
    this.entityService = args.entityService;
    this.recommendationsService = args.recommendationsService;
    this.statefulTokenService = args.statefulTokenService;
    this.secretService = args.secretService;
    this.entityRegistry = args.entityRegistry;
    this.gitVersion = args.gitVersion;
    this.supportsImpactAnalysis = args.supportsImpactAnalysis;
    this.timeseriesAspectService = args.timeseriesAspectService;
    this.timelineService = args.timelineService;
    this.nativeUserService = args.nativeUserService;
    this.groupService = args.groupService;
    this.roleService = args.roleService;
    this.inviteTokenService = args.inviteTokenService;
    this.postService = args.postService;
    this.viewService = args.viewService;
    this.ownershipTypeService = args.ownershipTypeService;
    this.settingsService = args.settingsService;
    this.lineageService = args.lineageService;
    this.queryService = args.queryService;
    this.erModelRelationshipService = args.erModelRelationshipService;
    this.dataProductService = args.dataProductService;
    this.formService = args.formService;
    this.restrictedService = args.restrictedService;
    this.connectionService = args.connectionService;
    this.assertionService = args.assertionService;
    this.entityVersioningService = args.entityVersioningService;

    this.businessAttributeService = args.businessAttributeService;
    this.ingestionConfiguration = Objects.requireNonNull(args.ingestionConfiguration);
    this.authenticationConfiguration = Objects.requireNonNull(args.authenticationConfiguration);
    this.authorizationConfiguration = Objects.requireNonNull(args.authorizationConfiguration);
    this.visualConfiguration = args.visualConfiguration;
    this.telemetryConfiguration = args.telemetryConfiguration;
    this.testsConfiguration = args.testsConfiguration;
    this.datahubConfiguration = args.datahubConfiguration;
    this.viewsConfiguration = args.viewsConfiguration;
    this.featureFlags = args.featureFlags;

    this.datasetType = new DatasetType(entityClient);
    this.roleType = new RoleType(entityClient);
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
    this.connectionType = new DataHubConnectionType(entityClient, secretService);
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
    this.schemaFieldType = new SchemaFieldType(entityClient, featureFlags);
    this.erModelRelationshipType = new ERModelRelationshipType(entityClient, featureFlags);
    this.dataHubViewType = new DataHubViewType(entityClient);
    this.queryType = new QueryType(entityClient);
    this.dataProductType = new DataProductType(entityClient);
    this.ownershipType = new OwnershipType(entityClient);
    this.structuredPropertyType = new StructuredPropertyType(entityClient);
    this.dataTypeType = new DataTypeType(entityClient);
    this.entityTypeType = new EntityTypeType(entityClient);
    this.formType = new FormType(entityClient);
    this.incidentType = new IncidentType(entityClient);
    this.restrictedType = new RestrictedType(entityClient, restrictedService);
    this.dataProcessInstanceType = new DataProcessInstanceType(entityClient, featureFlags);
    this.versionSetType = new VersionSetType(entityClient);

    this.graphQLQueryComplexityLimit = args.graphQLQueryComplexityLimit;
    this.graphQLQueryDepthLimit = args.graphQLQueryDepthLimit;
    this.graphQLQueryIntrospectionEnabled = args.graphQLQueryIntrospectionEnabled;

    this.businessAttributeType = new BusinessAttributeType(entityClient);
    // Init Lists
    this.entityTypes =
        new ArrayList<>(
            ImmutableList.of(
                datasetType,
                roleType,
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
                connectionType,
                containerType,
                notebookType,
                domainType,
                assertionType,
                versionedDatasetType,
                dataPlatformInstanceType,
                accessTokenMetadataType,
                testType,
                dataHubPolicyType,
                dataHubRoleType,
                schemaFieldType,
                erModelRelationshipType,
                dataHubViewType,
                queryType,
                dataProductType,
                ownershipType,
                structuredPropertyType,
                dataTypeType,
                entityTypeType,
                formType,
                incidentType,
                versionSetType,
                restrictedType,
                businessAttributeType,
                dataProcessInstanceType));
    this.loadableTypes = new ArrayList<>(entityTypes);
    // Extend loadable types with types from the plugins
    // This allows us to offer search and browse capabilities out of the box for
    // those types
    for (GmsGraphQLPlugin plugin : this.graphQLPlugins) {
      this.entityTypes.addAll(plugin.getEntityTypes());
      Collection<? extends LoadableType<?, ?>> pluginLoadableTypes = plugin.getLoadableTypes();
      if (pluginLoadableTypes != null) {
        this.loadableTypes.addAll(pluginLoadableTypes);
      }
    }
    this.ownerTypes = ImmutableList.of(corpUserType, corpGroupType);
    this.searchableTypes =
        loadableTypes.stream()
            .filter(type -> (type instanceof SearchableEntityType<?, ?>))
            .map(type -> (SearchableEntityType<?, ?>) type)
            .collect(Collectors.toList());
    this.browsableTypes =
        loadableTypes.stream()
            .filter(type -> (type instanceof BrowsableEntityType<?, ?>))
            .map(type -> (BrowsableEntityType<?, ?>) type)
            .collect(Collectors.toList());
  }

  /**
   * Returns a {@link Supplier} responsible for creating a new {@link DataLoader} from a {@link
   * LoadableType}.
   */
  public static Map<String, Function<QueryContext, DataLoader<?, ?>>> loaderSuppliers(
      final Collection<? extends LoadableType<?, ?>> loadableTypes) {
    return loadableTypes.stream()
        .collect(
            Collectors.toMap(
                LoadableType::name,
                (graphType) -> (context) -> createDataLoader(graphType, context)));
  }

  /**
   * Final call to wire up any extra resolvers the plugin might want to add on
   *
   * @param builder
   */
  private void configurePluginResolvers(final RuntimeWiring.Builder builder) {
    this.graphQLPlugins.forEach(plugin -> plugin.configureExtraResolvers(builder, this));
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
    configureOrganisationRoleResolvers(builder);
    configureGlossaryNodeResolvers(builder);
    configureDomainResolvers(builder);
    configureDataProductResolvers(builder);
    configureAssertionResolvers(builder);
    configureContractResolvers(builder);
    configurePolicyResolvers(builder);
    configureDataProcessInstanceResolvers(builder);
    configureVersionedDatasetResolvers(builder);
    configureAccessAccessTokenMetadataResolvers(builder);
    configureTestResultResolvers(builder);
    configureDataHubRoleResolvers(builder);
    configureSchemaFieldResolvers(builder);
    configureERModelRelationshipResolvers(builder);
    configureEntityPathResolvers(builder);
    configureResolvedAuditStampResolvers(builder);
    configureViewResolvers(builder);
    configureQueryEntityResolvers(builder);
    configureOwnershipTypeResolver(builder);
    configurePluginResolvers(builder);
    configureStructuredPropertyResolvers(builder);
    configureFormResolvers(builder);
    configureIncidentResolvers(builder);
    configureRestrictedResolvers(builder);
    configureRoleResolvers(builder);
    configureBusinessAttributeResolver(builder);
    configureBusinessAttributeAssociationResolver(builder);
    configureConnectionResolvers(builder);
    configureDeprecationResolvers(builder);
    configureMetadataAttributionResolver(builder);
    configureVersionPropertiesResolvers(builder);
    configureVersionSetResolvers(builder);
  }

  private void configureOrganisationRoleResolvers(RuntimeWiring.Builder builder) {
    builder.type(
        "Role",
        typeWiring ->
            typeWiring
                .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient))
                .dataFetcher(
                    "aspects", new WeaklyTypedAspectsResolver(entityClient, entityRegistry)));
    builder.type(
        "RoleAssociation",
        typeWiring ->
            typeWiring.dataFetcher(
                "role",
                new LoadableTypeResolver<>(
                    roleType,
                    (env) ->
                        ((com.linkedin.datahub.graphql.generated.RoleAssociation) env.getSource())
                            .getRole()
                            .getUrn())));
    builder.type(
        "RoleUser",
        typeWiring ->
            typeWiring.dataFetcher(
                "user",
                new LoadableTypeResolver<>(
                    corpUserType,
                    (env) ->
                        ((com.linkedin.datahub.graphql.generated.RoleUser) env.getSource())
                            .getUser()
                            .getUrn())));
  }

  public GraphQLEngine.Builder builder() {
    final GraphQLEngine.Builder builder = GraphQLEngine.builder();
    builder
        .addSchema(fileBasedSchema(GMS_SCHEMA_FILE))
        .addSchema(fileBasedSchema(SEARCH_SCHEMA_FILE))
        .addSchema(fileBasedSchema(APP_SCHEMA_FILE))
        .addSchema(fileBasedSchema(AUTH_SCHEMA_FILE))
        .addSchema(fileBasedSchema(ANALYTICS_SCHEMA_FILE))
        .addSchema(fileBasedSchema(RECOMMENDATIONS_SCHEMA_FILE))
        .addSchema(fileBasedSchema(INGESTION_SCHEMA_FILE))
        .addSchema(fileBasedSchema(TIMELINE_SCHEMA_FILE))
        .addSchema(fileBasedSchema(TESTS_SCHEMA_FILE))
        .addSchema(fileBasedSchema(STEPS_SCHEMA_FILE))
        .addSchema(fileBasedSchema(LINEAGE_SCHEMA_FILE))
        .addSchema(fileBasedSchema(PROPERTIES_SCHEMA_FILE))
        .addSchema(fileBasedSchema(FORMS_SCHEMA_FILE))
        .addSchema(fileBasedSchema(CONNECTIONS_SCHEMA_FILE))
        .addSchema(fileBasedSchema(ASSERTIONS_SCHEMA_FILE))
        .addSchema(fileBasedSchema(INCIDENTS_SCHEMA_FILE))
        .addSchema(fileBasedSchema(CONTRACTS_SCHEMA_FILE))
        .addSchema(fileBasedSchema(COMMON_SCHEMA_FILE))
        .addSchema(fileBasedSchema(VERSION_SCHEMA_FILE));

    for (GmsGraphQLPlugin plugin : this.graphQLPlugins) {
      List<String> pluginSchemaFiles = plugin.getSchemaFiles();
      if (pluginSchemaFiles != null) {
        pluginSchemaFiles.forEach(schema -> builder.addSchema(fileBasedSchema(schema)));
      }
      Collection<? extends LoadableType<?, ?>> pluginLoadableTypes = plugin.getLoadableTypes();
      if (pluginLoadableTypes != null) {
        pluginLoadableTypes.forEach(
            loadableType -> builder.addDataLoaders(loaderSuppliers(pluginLoadableTypes)));
      }
    }
    builder
        .addDataLoaders(loaderSuppliers(loadableTypes))
        .addDataLoader("Aspect", context -> createDataLoader(aspectType, context))
        .setGraphQLQueryComplexityLimit(graphQLQueryComplexityLimit)
        .setGraphQLQueryDepthLimit(graphQLQueryDepthLimit)
        .setGraphQLQueryIntrospectionEnabled(graphQLQueryIntrospectionEnabled)
        .configureRuntimeWiring(this::configureRuntimeWiring);
    return builder;
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
    builder
        .type(
            "Query",
            typeWiring ->
                typeWiring.dataFetcher(
                    "isAnalyticsEnabled", new IsAnalyticsEnabledResolver(isAnalyticsEnabled)))
        .type(
            "AnalyticsChart",
            typeWiring -> typeWiring.typeResolver(new AnalyticsChartTypeResolver()));
    if (isAnalyticsEnabled) {
      builder.type(
          "Query",
          typeWiring ->
              typeWiring
                  .dataFetcher(
                      "getAnalyticsCharts", new GetChartsResolver(analyticsService, entityClient))
                  .dataFetcher("getHighlights", new GetHighlightsResolver(analyticsService))
                  .dataFetcher(
                      "getMetadataAnalyticsCharts",
                      new GetMetadataAnalyticsResolver(entityClient)));
    }
  }

  private void configureContainerResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "Container",
        typeWiring ->
            typeWiring
                .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient))
                .dataFetcher("entities", new ContainerEntitiesResolver(entityClient))
                .dataFetcher("privileges", new EntityPrivilegesResolver(entityClient))
                .dataFetcher(
                    "aspects", new WeaklyTypedAspectsResolver(entityClient, entityRegistry))
                .dataFetcher("exists", new EntityExistsResolver(entityService))
                .dataFetcher(
                    "platform",
                    new LoadableTypeResolver<>(
                        dataPlatformType,
                        (env) -> ((Container) env.getSource()).getPlatform().getUrn()))
                .dataFetcher(
                    "container",
                    new LoadableTypeResolver<>(
                        containerType,
                        (env) -> {
                          final Container container = env.getSource();
                          return container.getContainer() != null
                              ? container.getContainer().getUrn()
                              : null;
                        }))
                .dataFetcher("parentContainers", new ParentContainersResolver(entityClient))
                .dataFetcher(
                    "dataPlatformInstance",
                    new LoadableTypeResolver<>(
                        dataPlatformInstanceType,
                        (env) -> {
                          final Container container = env.getSource();
                          return container.getDataPlatformInstance() != null
                              ? container.getDataPlatformInstance().getUrn()
                              : null;
                        })));
  }

  private void configureDataPlatformInstanceResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "DataPlatformInstance",
        typeWiring ->
            typeWiring.dataFetcher(
                "platform",
                new LoadableTypeResolver<>(
                    dataPlatformType,
                    (env) -> ((DataPlatformInstance) env.getSource()).getPlatform().getUrn())));
  }

  private void configureQueryResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "Query",
        typeWiring ->
            typeWiring
                .dataFetcher(
                    "appConfig",
                    new AppConfigResolver(
                        gitVersion,
                        analyticsService != null,
                        this.ingestionConfiguration,
                        this.authenticationConfiguration,
                        this.authorizationConfiguration,
                        this.supportsImpactAnalysis,
                        this.visualConfiguration,
                        this.telemetryConfiguration,
                        this.testsConfiguration,
                        this.datahubConfiguration,
                        this.viewsConfiguration,
                        this.featureFlags))
                .dataFetcher("me", new MeResolver(this.entityClient, featureFlags))
                .dataFetcher("search", new SearchResolver(this.entityClient))
                .dataFetcher(
                    "searchAcrossEntities",
                    new SearchAcrossEntitiesResolver(this.entityClient, this.viewService))
                .dataFetcher(
                    "scrollAcrossEntities",
                    new ScrollAcrossEntitiesResolver(this.entityClient, this.viewService))
                .dataFetcher(
                    "searchAcrossLineage",
                    new SearchAcrossLineageResolver(this.entityClient, this.entityRegistry))
                .dataFetcher(
                    "scrollAcrossLineage", new ScrollAcrossLineageResolver(this.entityClient))
                .dataFetcher(
                    "aggregateAcrossEntities",
                    new AggregateAcrossEntitiesResolver(
                        this.entityClient, this.viewService, this.formService))
                .dataFetcher("autoComplete", new AutoCompleteResolver(searchableTypes))
                .dataFetcher(
                    "autoCompleteForMultiple",
                    new AutoCompleteForMultipleResolver(searchableTypes, this.viewService))
                .dataFetcher("browse", new BrowseResolver(browsableTypes))
                .dataFetcher("browsePaths", new BrowsePathsResolver(browsableTypes))
                .dataFetcher("dataset", getResolver(datasetType))
                .dataFetcher("role", getResolver(roleType))
                .dataFetcher(
                    "versionedDataset",
                    getResolver(
                        versionedDatasetType,
                        (env) ->
                            new VersionedUrn()
                                .setUrn(UrnUtils.getUrn(env.getArgument(URN_FIELD_NAME)))
                                .setVersionStamp(env.getArgument(VERSION_STAMP_FIELD_NAME))))
                .dataFetcher("notebook", getResolver(notebookType))
                .dataFetcher("corpUser", getResolver(corpUserType))
                .dataFetcher("corpGroup", getResolver(corpGroupType))
                .dataFetcher("dashboard", getResolver(dashboardType))
                .dataFetcher("chart", getResolver(chartType))
                .dataFetcher("tag", getResolver(tagType))
                .dataFetcher("dataFlow", getResolver(dataFlowType))
                .dataFetcher("dataJob", getResolver(dataJobType))
                .dataFetcher("dataProcessInstance", getResolver(dataProcessInstanceType))
                .dataFetcher("glossaryTerm", getResolver(glossaryTermType))
                .dataFetcher("glossaryNode", getResolver(glossaryNodeType))
                .dataFetcher("domain", getResolver((domainType)))
                .dataFetcher("erModelRelationship", getResolver(erModelRelationshipType))
                .dataFetcher("dataPlatform", getResolver(dataPlatformType))
                .dataFetcher("dataPlatformInstance", getResolver(dataPlatformInstanceType))
                .dataFetcher("mlFeatureTable", getResolver(mlFeatureTableType))
                .dataFetcher("mlFeature", getResolver(mlFeatureType))
                .dataFetcher("mlPrimaryKey", getResolver(mlPrimaryKeyType))
                .dataFetcher("mlModel", getResolver(mlModelType))
                .dataFetcher("mlModelGroup", getResolver(mlModelGroupType))
                .dataFetcher("assertion", getResolver(assertionType))
                .dataFetcher("form", getResolver(formType))
                .dataFetcher("view", getResolver(dataHubViewType))
                .dataFetcher("structuredProperty", getResolver(structuredPropertyType))
                .dataFetcher("versionSet", getResolver(versionSetType))
                .dataFetcher("listPolicies", new ListPoliciesResolver(this.entityClient))
                .dataFetcher("getGrantedPrivileges", new GetGrantedPrivilegesResolver())
                .dataFetcher("listUsers", new ListUsersResolver(this.entityClient))
                .dataFetcher("listGroups", new ListGroupsResolver(this.entityClient))
                .dataFetcher(
                    "listRecommendations",
                    new ListRecommendationsResolver(recommendationsService, viewService))
                .dataFetcher(
                    "getEntityCounts", new EntityCountsResolver(this.entityClient, viewService))
                .dataFetcher("getAccessToken", new GetAccessTokenResolver(statefulTokenService))
                .dataFetcher("listAccessTokens", new ListAccessTokensResolver(this.entityClient))
                .dataFetcher(
                    "getAccessTokenMetadata",
                    new GetAccessTokenMetadataResolver(statefulTokenService, this.entityClient))
                .dataFetcher("debugAccess", new DebugAccessResolver(this.entityClient, graphClient))
                .dataFetcher("container", getResolver(containerType))
                .dataFetcher("listDomains", new ListDomainsResolver(this.entityClient))
                .dataFetcher("listSecrets", new ListSecretsResolver(this.entityClient))
                .dataFetcher(
                    "getSecretValues",
                    new GetSecretValuesResolver(this.entityClient, this.secretService))
                .dataFetcher(
                    "listIngestionSources", new ListIngestionSourcesResolver(this.entityClient))
                .dataFetcher("ingestionSource", new GetIngestionSourceResolver(this.entityClient))
                .dataFetcher(
                    "executionRequest", new GetIngestionExecutionRequestResolver(this.entityClient))
                .dataFetcher("getSchemaBlame", new GetSchemaBlameResolver(this.timelineService))
                .dataFetcher(
                    "getSchemaVersionList", new GetSchemaVersionListResolver(this.timelineService))
                .dataFetcher("test", getResolver(testType))
                .dataFetcher("listTests", new ListTestsResolver(entityClient))
                .dataFetcher(
                    "getRootGlossaryTerms", new GetRootGlossaryTermsResolver(this.entityClient))
                .dataFetcher(
                    "getRootGlossaryNodes", new GetRootGlossaryNodesResolver(this.entityClient))
                .dataFetcher("entityExists", new EntityExistsResolver(this.entityService))
                .dataFetcher("entity", getEntityResolver())
                .dataFetcher("entities", getEntitiesResolver())
                .dataFetcher("listRoles", new ListRolesResolver(this.entityClient))
                .dataFetcher("getInviteToken", new GetInviteTokenResolver(this.inviteTokenService))
                .dataFetcher("listPosts", new ListPostsResolver(this.entityClient))
                .dataFetcher(
                    "batchGetStepStates", new BatchGetStepStatesResolver(this.entityClient))
                .dataFetcher("listMyViews", new ListMyViewsResolver(this.entityClient))
                .dataFetcher("listGlobalViews", new ListGlobalViewsResolver(this.entityClient))
                .dataFetcher(
                    "globalViewsSettings", new GlobalViewsSettingsResolver(this.settingsService))
                .dataFetcher("listQueries", new ListQueriesResolver(this.entityClient))
                .dataFetcher(
                    "getQuickFilters",
                    new GetQuickFiltersResolver(this.entityClient, this.viewService))
                .dataFetcher("dataProduct", getResolver(dataProductType))
                .dataFetcher(
                    "listDataProductAssets", new ListDataProductAssetsResolver(this.entityClient))
                .dataFetcher(
                    "listOwnershipTypes", new ListOwnershipTypesResolver(this.entityClient))
                .dataFetcher(
                    "browseV2",
                    new BrowseV2Resolver(this.entityClient, this.viewService, this.formService))
                .dataFetcher("businessAttribute", getResolver(businessAttributeType))
                .dataFetcher(
                    "listBusinessAttributes", new ListBusinessAttributesResolver(this.entityClient))
                .dataFetcher(
                    "docPropagationSettings",
                    new DocPropagationSettingsResolver(this.settingsService)));
  }

  private DataFetcher getEntitiesResolver() {
    return new BatchGetEntitiesResolver(
        entityTypes,
        (env) -> {
          final QueryContext context = env.getContext();
          List<String> urns = env.getArgument(URNS_FIELD_NAME);
          return urns.stream()
              .map(UrnUtils::getUrn)
              .map(
                  (urn) -> {
                    try {
                      return UrnToEntityMapper.map(context, urn);
                    } catch (Exception e) {
                      throw new RuntimeException("Failed to get entity", e);
                    }
                  })
              .collect(Collectors.toList());
        });
  }

  private DataFetcher getEntityResolver() {
    return new EntityTypeResolver(
        entityTypes,
        (env) -> {
          try {
            final QueryContext context = env.getContext();
            Urn urn = Urn.createFromString(env.getArgument(URN_FIELD_NAME));
            return UrnToEntityMapper.map(context, urn);
          } catch (Exception e) {
            throw new RuntimeException("Failed to get entity", e);
          }
        });
  }

  private static DataFetcher getResolver(LoadableType<?, String> loadableType) {
    return getResolver(loadableType, GmsGraphQLEngine::getUrnField);
  }

  private static <T, K> DataFetcher getResolver(
      LoadableType<T, K> loadableType, Function<DataFetchingEnvironment, K> keyProvider) {
    return new LoadableTypeResolver<>(loadableType, keyProvider);
  }

  private static String getUrnField(DataFetchingEnvironment env) {
    return env.getArgument(URN_FIELD_NAME);
  }

  private void configureMutationResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "Mutation",
        typeWiring -> {
          typeWiring
              .dataFetcher("updateDataset", new MutableTypeResolver<>(datasetType))
              .dataFetcher("updateDatasets", new MutableTypeBatchResolver<>(datasetType))
              .dataFetcher(
                  "createTag", new CreateTagResolver(this.entityClient, this.entityService))
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
              .dataFetcher(
                  "updateERModelRelationship",
                  new UpdateERModelRelationshipResolver(this.entityClient))
              .dataFetcher(
                  "createERModelRelationship",
                  new CreateERModelRelationshipResolver(
                      this.entityClient, this.erModelRelationshipService))
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
              .dataFetcher(
                  "updateDescription",
                  new UpdateDescriptionResolver(entityService, this.entityClient))
              .dataFetcher("addOwner", new AddOwnerResolver(entityService, entityClient))
              .dataFetcher("addOwners", new AddOwnersResolver(entityService, entityClient))
              .dataFetcher(
                  "batchAddOwners", new BatchAddOwnersResolver(entityService, entityClient))
              .dataFetcher("removeOwner", new RemoveOwnerResolver(entityService, entityClient))
              .dataFetcher(
                  "batchRemoveOwners", new BatchRemoveOwnersResolver(entityService, entityClient))
              .dataFetcher("addLink", new AddLinkResolver(entityService, this.entityClient))
              .dataFetcher("removeLink", new RemoveLinkResolver(entityService, entityClient))
              .dataFetcher("addGroupMembers", new AddGroupMembersResolver(this.groupService))
              .dataFetcher("removeGroupMembers", new RemoveGroupMembersResolver(this.groupService))
              .dataFetcher("createGroup", new CreateGroupResolver(this.groupService))
              .dataFetcher("removeUser", new RemoveUserResolver(this.entityClient))
              .dataFetcher("removeGroup", new RemoveGroupResolver(this.entityClient))
              .dataFetcher("updateUserStatus", new UpdateUserStatusResolver(this.entityClient))
              .dataFetcher(
                  "createDomain", new CreateDomainResolver(this.entityClient, this.entityService))
              .dataFetcher(
                  "moveDomain", new MoveDomainResolver(this.entityService, this.entityClient))
              .dataFetcher("deleteDomain", new DeleteDomainResolver(entityClient))
              .dataFetcher(
                  "setDomain", new SetDomainResolver(this.entityClient, this.entityService))
              .dataFetcher(
                  "batchSetDomain", new BatchSetDomainResolver(this.entityService, entityClient))
              .dataFetcher(
                  "updateDeprecation",
                  new UpdateDeprecationResolver(this.entityClient, this.entityService))
              .dataFetcher(
                  "batchUpdateDeprecation", new BatchUpdateDeprecationResolver(entityService))
              .dataFetcher(
                  "unsetDomain", new UnsetDomainResolver(this.entityClient, this.entityService))
              .dataFetcher(
                  "createSecret", new CreateSecretResolver(this.entityClient, this.secretService))
              .dataFetcher("deleteSecret", new DeleteSecretResolver(this.entityClient))
              .dataFetcher(
                  "updateSecret", new UpdateSecretResolver(this.entityClient, this.secretService))
              .dataFetcher(
                  "createAccessToken", new CreateAccessTokenResolver(this.statefulTokenService))
              .dataFetcher(
                  "revokeAccessToken",
                  new RevokeAccessTokenResolver(this.entityClient, this.statefulTokenService))
              .dataFetcher(
                  "createIngestionSource", new UpsertIngestionSourceResolver(this.entityClient))
              .dataFetcher(
                  "updateIngestionSource", new UpsertIngestionSourceResolver(this.entityClient))
              .dataFetcher(
                  "deleteIngestionSource", new DeleteIngestionSourceResolver(this.entityClient))
              .dataFetcher(
                  "createIngestionExecutionRequest",
                  new CreateIngestionExecutionRequestResolver(
                      this.entityClient, this.ingestionConfiguration))
              .dataFetcher(
                  "cancelIngestionExecutionRequest",
                  new CancelIngestionExecutionRequestResolver(this.entityClient))
              .dataFetcher(
                  "createTestConnectionRequest",
                  new CreateTestConnectionRequestResolver(
                      this.entityClient, this.ingestionConfiguration))
              .dataFetcher(
                  "upsertCustomAssertion", new UpsertCustomAssertionResolver(assertionService))
              .dataFetcher(
                  "reportAssertionResult", new ReportAssertionResultResolver(assertionService))
              .dataFetcher(
                  "deleteAssertion",
                  new DeleteAssertionResolver(this.entityClient, this.entityService))
              .dataFetcher("createTest", new CreateTestResolver(this.entityClient))
              .dataFetcher("updateTest", new UpdateTestResolver(this.entityClient))
              .dataFetcher("deleteTest", new DeleteTestResolver(this.entityClient))
              .dataFetcher("reportOperation", new ReportOperationResolver(this.entityClient))
              .dataFetcher(
                  "createGlossaryTerm",
                  new CreateGlossaryTermResolver(this.entityClient, this.entityService))
              .dataFetcher(
                  "createGlossaryNode",
                  new CreateGlossaryNodeResolver(this.entityClient, this.entityService))
              .dataFetcher(
                  "updateParentNode",
                  new UpdateParentNodeResolver(this.entityService, this.entityClient))
              .dataFetcher(
                  "deleteGlossaryEntity",
                  new DeleteGlossaryEntityResolver(this.entityClient, this.entityService))
              .dataFetcher(
                  "updateName", new UpdateNameResolver(this.entityService, this.entityClient))
              .dataFetcher(
                  "addRelatedTerms",
                  new AddRelatedTermsResolver(this.entityService, this.entityClient))
              .dataFetcher(
                  "removeRelatedTerms",
                  new RemoveRelatedTermsResolver(this.entityService, this.entityClient))
              .dataFetcher(
                  "createNativeUserResetToken",
                  new CreateNativeUserResetTokenResolver(this.nativeUserService))
              .dataFetcher(
                  "batchUpdateSoftDeleted", new BatchUpdateSoftDeletedResolver(this.entityService))
              .dataFetcher("updateUserSetting", new UpdateUserSettingResolver(this.entityService))
              .dataFetcher("rollbackIngestion", new RollbackIngestionResolver(this.entityClient))
              .dataFetcher("batchAssignRole", new BatchAssignRoleResolver(this.roleService))
              .dataFetcher(
                  "createInviteToken", new CreateInviteTokenResolver(this.inviteTokenService))
              .dataFetcher(
                  "acceptRole", new AcceptRoleResolver(this.roleService, this.inviteTokenService))
              .dataFetcher("createPost", new CreatePostResolver(this.postService))
              .dataFetcher("deletePost", new DeletePostResolver(this.postService))
              .dataFetcher("updatePost", new UpdatePostResolver(this.postService))
              .dataFetcher(
                  "batchUpdateStepStates", new BatchUpdateStepStatesResolver(this.entityClient))
              .dataFetcher("createView", new CreateViewResolver(this.viewService))
              .dataFetcher("updateView", new UpdateViewResolver(this.viewService))
              .dataFetcher("deleteView", new DeleteViewResolver(this.viewService))
              .dataFetcher(
                  "updateGlobalViewsSettings",
                  new UpdateGlobalViewsSettingsResolver(this.settingsService))
              .dataFetcher(
                  "updateCorpUserViewsSettings",
                  new UpdateCorpUserViewsSettingsResolver(this.settingsService))
              .dataFetcher(
                  "updateLineage",
                  new UpdateLineageResolver(this.entityService, this.lineageService))
              .dataFetcher("updateEmbed", new UpdateEmbedResolver(this.entityService))
              .dataFetcher("createQuery", new CreateQueryResolver(this.queryService))
              .dataFetcher("updateQuery", new UpdateQueryResolver(this.queryService))
              .dataFetcher("deleteQuery", new DeleteQueryResolver(this.queryService))
              .dataFetcher(
                  "createDataProduct",
                  new CreateDataProductResolver(this.dataProductService, this.entityService))
              .dataFetcher(
                  "updateDataProduct", new UpdateDataProductResolver(this.dataProductService))
              .dataFetcher(
                  "deleteDataProduct", new DeleteDataProductResolver(this.dataProductService))
              .dataFetcher(
                  "batchSetDataProduct", new BatchSetDataProductResolver(this.dataProductService))
              .dataFetcher(
                  "createOwnershipType", new CreateOwnershipTypeResolver(this.ownershipTypeService))
              .dataFetcher(
                  "updateOwnershipType", new UpdateOwnershipTypeResolver(this.ownershipTypeService))
              .dataFetcher(
                  "deleteOwnershipType", new DeleteOwnershipTypeResolver(this.ownershipTypeService))
              .dataFetcher("submitFormPrompt", new SubmitFormPromptResolver(this.formService))
              .dataFetcher("batchAssignForm", new BatchAssignFormResolver(this.formService))
              .dataFetcher(
                  "createDynamicFormAssignment",
                  new CreateDynamicFormAssignmentResolver(this.formService))
              .dataFetcher(
                  "verifyForm", new VerifyFormResolver(this.formService, this.groupService))
              .dataFetcher("batchRemoveForm", new BatchRemoveFormResolver(this.formService))
              .dataFetcher(
                  "upsertStructuredProperties",
                  new UpsertStructuredPropertiesResolver(this.entityClient))
              .dataFetcher(
                  "removeStructuredProperties",
                  new RemoveStructuredPropertiesResolver(this.entityClient))
              .dataFetcher(
                  "createStructuredProperty",
                  new CreateStructuredPropertyResolver(this.entityClient))
              .dataFetcher(
                  "updateStructuredProperty",
                  new UpdateStructuredPropertyResolver(this.entityClient))
              .dataFetcher(
                  "deleteStructuredProperty",
                  new DeleteStructuredPropertyResolver(this.entityClient))
              .dataFetcher("raiseIncident", new RaiseIncidentResolver(this.entityClient))
              .dataFetcher(
                  "updateIncidentStatus",
                  new UpdateIncidentStatusResolver(this.entityClient, this.entityService))
              .dataFetcher(
                  "createForm", new CreateFormResolver(this.entityClient, this.formService))
              .dataFetcher("deleteForm", new DeleteFormResolver(this.entityClient))
              .dataFetcher("updateForm", new UpdateFormResolver(this.entityClient))
              .dataFetcher(
                  "updateDocPropagationSettings",
                  new UpdateDocPropagationSettingsResolver(this.settingsService));

          if (featureFlags.isBusinessAttributeEntityEnabled()) {
            typeWiring
                .dataFetcher(
                    "createBusinessAttribute",
                    new CreateBusinessAttributeResolver(
                        this.entityClient, this.entityService, this.businessAttributeService))
                .dataFetcher(
                    "updateBusinessAttribute",
                    new UpdateBusinessAttributeResolver(
                        this.entityClient, this.businessAttributeService))
                .dataFetcher(
                    "deleteBusinessAttribute",
                    new DeleteBusinessAttributeResolver(this.entityClient))
                .dataFetcher(
                    "addBusinessAttribute", new AddBusinessAttributeResolver(this.entityService))
                .dataFetcher(
                    "removeBusinessAttribute",
                    new RemoveBusinessAttributeResolver(this.entityService));
          }
          if (featureFlags.isEntityVersioning()) {
            typeWiring
                .dataFetcher(
                    "linkAssetVersion",
                    new LinkAssetVersionResolver(this.entityVersioningService, this.featureFlags))
                .dataFetcher(
                    "unlinkAssetVersion",
                    new UnlinkAssetVersionResolver(
                        this.entityVersioningService, this.featureFlags));
          }
          return typeWiring;
        });
  }

  private void configureGenericEntityResolvers(final RuntimeWiring.Builder builder) {
    builder
        .type(
            "SearchResult",
            typeWiring ->
                typeWiring.dataFetcher(
                    "entity",
                    new EntityTypeResolver(
                        entityTypes, (env) -> ((SearchResult) env.getSource()).getEntity())))
        .type(
            "MatchedField",
            typeWiring ->
                typeWiring.dataFetcher(
                    "entity",
                    new EntityTypeResolver(
                        entityTypes, (env) -> ((MatchedField) env.getSource()).getEntity())))
        .type(
            "SearchAcrossLineageResult",
            typeWiring ->
                typeWiring.dataFetcher(
                    "entity",
                    new EntityTypeResolver(
                        entityTypes,
                        (env) -> ((SearchAcrossLineageResult) env.getSource()).getEntity())))
        .type(
            "AggregationMetadata",
            typeWiring ->
                typeWiring.dataFetcher(
                    "entity",
                    new EntityTypeResolver(
                        entityTypes, (env) -> ((AggregationMetadata) env.getSource()).getEntity())))
        .type(
            "RecommendationContent",
            typeWiring ->
                typeWiring.dataFetcher(
                    "entity",
                    new EntityTypeResolver(
                        entityTypes,
                        (env) -> ((RecommendationContent) env.getSource()).getEntity())))
        .type(
            "BrowseResults",
            typeWiring ->
                typeWiring.dataFetcher(
                    "entities",
                    new EntityTypeBatchResolver(
                        entityTypes, (env) -> ((BrowseResults) env.getSource()).getEntities())))
        .type(
            "ParentDomainsResult",
            typeWiring ->
                typeWiring.dataFetcher(
                    "domains",
                    new EntityTypeBatchResolver(
                        entityTypes,
                        (env) -> {
                          final ParentDomainsResult result = env.getSource();
                          return result != null ? result.getDomains() : null;
                        })))
        .type(
            "EntityRelationshipLegacy",
            typeWiring ->
                typeWiring.dataFetcher(
                    "entity",
                    new EntityTypeResolver(
                        entityTypes,
                        (env) -> ((EntityRelationshipLegacy) env.getSource()).getEntity())))
        .type(
            "EntityRelationship",
            typeWiring ->
                typeWiring.dataFetcher(
                    "entity",
                    new EntityTypeResolver(
                        entityTypes, (env) -> ((EntityRelationship) env.getSource()).getEntity())))
        .type(
            "BrowseResultGroupV2",
            typeWiring ->
                typeWiring.dataFetcher(
                    "entity",
                    new EntityTypeResolver(
                        entityTypes, (env) -> ((BrowseResultGroupV2) env.getSource()).getEntity())))
        .type(
            "BrowsePathEntry",
            typeWiring ->
                typeWiring.dataFetcher(
                    "entity",
                    new EntityTypeResolver(
                        entityTypes, (env) -> ((BrowsePathEntry) env.getSource()).getEntity())))
        .type(
            "FacetMetadata",
            typeWiring ->
                typeWiring.dataFetcher(
                    "entity",
                    new EntityTypeResolver(
                        entityTypes,
                        (env) -> {
                          FacetMetadata facetMetadata = env.getSource();
                          return facetMetadata.getEntity() != null
                              ? facetMetadata.getEntity()
                              : null;
                        })))
        .type(
            "LineageRelationship",
            typeWiring ->
                typeWiring
                    .dataFetcher(
                        "entity",
                        new EntityTypeResolver(
                            entityTypes,
                            (env) -> ((LineageRelationship) env.getSource()).getEntity()))
                    .dataFetcher(
                        "createdActor",
                        new EntityTypeResolver(
                            entityTypes,
                            (env) -> {
                              final LineageRelationship relationship = env.getSource();
                              return relationship.getCreatedActor() != null
                                  ? relationship.getCreatedActor()
                                  : null;
                            }))
                    .dataFetcher(
                        "updatedActor",
                        new EntityTypeResolver(
                            entityTypes,
                            (env) -> {
                              final LineageRelationship relationship = env.getSource();
                              return relationship.getUpdatedActor() != null
                                  ? relationship.getUpdatedActor()
                                  : null;
                            })))
        .type(
            "ListDomainsResult",
            typeWiring ->
                typeWiring.dataFetcher(
                    "domains",
                    new LoadableTypeBatchResolver<>(
                        domainType,
                        (env) ->
                            ((ListDomainsResult) env.getSource())
                                .getDomains().stream()
                                    .map(Domain::getUrn)
                                    .collect(Collectors.toList()))))
        .type(
            "GetRootGlossaryTermsResult",
            typeWiring ->
                typeWiring.dataFetcher(
                    "terms",
                    new LoadableTypeBatchResolver<>(
                        glossaryTermType,
                        (env) ->
                            ((GetRootGlossaryTermsResult) env.getSource())
                                .getTerms().stream()
                                    .map(GlossaryTerm::getUrn)
                                    .collect(Collectors.toList()))))
        .type(
            "GetRootGlossaryNodesResult",
            typeWiring ->
                typeWiring.dataFetcher(
                    "nodes",
                    new LoadableTypeBatchResolver<>(
                        glossaryNodeType,
                        (env) ->
                            ((GetRootGlossaryNodesResult) env.getSource())
                                .getNodes().stream()
                                    .map(GlossaryNode::getUrn)
                                    .collect(Collectors.toList()))))
        .type(
            "AutoCompleteResults",
            typeWiring ->
                typeWiring.dataFetcher(
                    "entities",
                    new EntityTypeBatchResolver(
                        entityTypes,
                        (env) -> ((AutoCompleteResults) env.getSource()).getEntities())))
        .type(
            "AutoCompleteResultForEntity",
            typeWiring ->
                typeWiring.dataFetcher(
                    "entities",
                    new EntityTypeBatchResolver(
                        entityTypes,
                        (env) -> ((AutoCompleteResultForEntity) env.getSource()).getEntities())))
        .type(
            "PolicyMatchCriterionValue",
            typeWiring ->
                typeWiring.dataFetcher(
                    "entity",
                    new EntityTypeResolver(
                        entityTypes,
                        (env) -> ((PolicyMatchCriterionValue) env.getSource()).getEntity())))
        .type(
            "ListTestsResult",
            typeWiring ->
                typeWiring.dataFetcher(
                    "tests",
                    new LoadableTypeBatchResolver<>(
                        testType,
                        (env) ->
                            ((ListTestsResult) env.getSource())
                                .getTests().stream()
                                    .map(Test::getUrn)
                                    .collect(Collectors.toList()))))
        .type(
            "QuickFilter",
            typeWiring ->
                typeWiring.dataFetcher(
                    "entity",
                    new EntityTypeResolver(
                        entityTypes, (env) -> ((QuickFilter) env.getSource()).getEntity())))
        .type(
            "Owner",
            typeWiring ->
                typeWiring.dataFetcher(
                    "ownershipType",
                    new EntityTypeResolver(
                        entityTypes, (env) -> ((Owner) env.getSource()).getOwnershipType())))
        .type(
            "StructuredPropertiesEntry",
            typeWiring ->
                typeWiring
                    .dataFetcher(
                        "structuredProperty",
                        new LoadableTypeResolver<>(
                            structuredPropertyType,
                            (env) ->
                                ((StructuredPropertiesEntry) env.getSource())
                                    .getStructuredProperty()
                                    .getUrn()))
                    .dataFetcher(
                        "valueEntities",
                        new BatchGetEntitiesResolver(
                            entityTypes,
                            (env) ->
                                ((StructuredPropertiesEntry) env.getSource()).getValueEntities())));
  }

  /**
   * Configures resolvers responsible for resolving the {@link
   * com.linkedin.datahub.graphql.generated.Dataset} type.
   */
  private void configureDatasetResolvers(final RuntimeWiring.Builder builder) {
    builder
        .type(
            "Dataset",
            typeWiring ->
                typeWiring
                    .dataFetcher(
                        "relationships", new EntityRelationshipsResultResolver(graphClient))
                    .dataFetcher("browsePaths", new EntityBrowsePathsResolver(this.datasetType))
                    .dataFetcher(
                        "lineage",
                        new EntityLineageResultResolver(
                            siblingGraphService,
                            restrictedService,
                            this.authorizationConfiguration))
                    .dataFetcher(
                        "platform",
                        new LoadableTypeResolver<>(
                            dataPlatformType,
                            (env) -> ((Dataset) env.getSource()).getPlatform().getUrn()))
                    .dataFetcher(
                        "container",
                        new LoadableTypeResolver<>(
                            containerType,
                            (env) -> {
                              final Dataset dataset = env.getSource();
                              return dataset.getContainer() != null
                                  ? dataset.getContainer().getUrn()
                                  : null;
                            }))
                    .dataFetcher(
                        "dataPlatformInstance",
                        new LoadableTypeResolver<>(
                            dataPlatformInstanceType,
                            (env) -> {
                              final Dataset dataset = env.getSource();
                              return dataset.getDataPlatformInstance() != null
                                  ? dataset.getDataPlatformInstance().getUrn()
                                  : null;
                            }))
                    .dataFetcher(
                        "datasetProfiles",
                        new TimeSeriesAspectResolver(
                            this.entityClient,
                            "dataset",
                            "datasetProfile",
                            DatasetProfileMapper::map))
                    .dataFetcher(
                        "operations",
                        new TimeSeriesAspectResolver(
                            this.entityClient,
                            "dataset",
                            "operation",
                            OperationMapper::map,
                            new SortCriterion()
                                .setField(OPERATION_EVENT_TIME_FIELD_NAME)
                                .setOrder(SortOrder.DESCENDING)))
                    .dataFetcher("usageStats", new DatasetUsageStatsResolver(this.usageClient))
                    .dataFetcher("statsSummary", new DatasetStatsSummaryResolver(this.usageClient))
                    .dataFetcher(
                        "health",
                        new EntityHealthResolver(
                            entityClient,
                            graphClient,
                            timeseriesAspectService,
                            new EntityHealthResolver.Config(true, true)))
                    .dataFetcher("schemaMetadata", new AspectResolver())
                    .dataFetcher(
                        "assertions", new EntityAssertionsResolver(entityClient, graphClient))
                    .dataFetcher("testResults", new TestResultsResolver(entityClient))
                    .dataFetcher(
                        "aspects", new WeaklyTypedAspectsResolver(entityClient, entityRegistry))
                    .dataFetcher("exists", new EntityExistsResolver(entityService))
                    .dataFetcher("runs", new EntityRunsResolver(entityClient))
                    .dataFetcher("privileges", new EntityPrivilegesResolver(entityClient))
                    .dataFetcher("parentContainers", new ParentContainersResolver(entityClient)))
        .type(
            "Owner",
            typeWiring ->
                typeWiring.dataFetcher(
                    "owner",
                    new OwnerTypeResolver<>(
                        ownerTypes, (env) -> ((Owner) env.getSource()).getOwner())))
        .type(
            "SchemaField",
            typeWiring ->
                typeWiring.dataFetcher(
                    "schemaFieldEntity",
                    new LoadableTypeResolver<>(
                        schemaFieldType,
                        (env) -> ((SchemaField) env.getSource()).getSchemaFieldEntity().getUrn())))
        .type(
            "UserUsageCounts",
            typeWiring ->
                typeWiring.dataFetcher(
                    "user",
                    new LoadableTypeResolver<>(
                        corpUserType,
                        (env) -> ((UserUsageCounts) env.getSource()).getUser().getUrn())))
        .type(
            "ForeignKeyConstraint",
            typeWiring ->
                typeWiring.dataFetcher(
                    "foreignDataset",
                    new LoadableTypeResolver<>(
                        datasetType,
                        (env) ->
                            ((ForeignKeyConstraint) env.getSource()).getForeignDataset().getUrn())))
        .type(
            "SiblingProperties",
            typeWiring ->
                typeWiring.dataFetcher(
                    "siblings",
                    new EntityTypeBatchResolver(
                        new ArrayList<>(entityTypes),
                        (env) -> ((SiblingProperties) env.getSource()).getSiblings())))
        .type(
            "InstitutionalMemoryMetadata",
            typeWiring ->
                typeWiring
                    .dataFetcher(
                        "author",
                        new LoadableTypeResolver<>(
                            corpUserType,
                            (env) ->
                                ((InstitutionalMemoryMetadata) env.getSource())
                                    .getAuthor()
                                    .getUrn()))
                    .dataFetcher(
                        "actor",
                        new EntityTypeResolver(
                            this.entityTypes,
                            (env) ->
                                (Entity)
                                    ((InstitutionalMemoryMetadata) env.getSource()).getActor())))
        .type(
            "DatasetStatsSummary",
            typeWiring ->
                typeWiring.dataFetcher(
                    "topUsersLast30Days",
                    new LoadableTypeBatchResolver<>(
                        corpUserType,
                        (env) -> {
                          DatasetStatsSummary summary = ((DatasetStatsSummary) env.getSource());
                          return summary.getTopUsersLast30Days() != null
                              ? summary.getTopUsersLast30Days().stream()
                                  .map(CorpUser::getUrn)
                                  .collect(Collectors.toList())
                              : null;
                        })));
  }

  /**
   * Configures resolvers responsible for resolving the {@link
   * com.linkedin.datahub.graphql.generated.VersionedDataset} type.
   */
  private void configureVersionedDatasetResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "VersionedDataset",
        typeWiring -> typeWiring.dataFetcher("relationships", new StaticDataFetcher(null)));
  }

  /**
   * Configures resolvers responsible for resolving the {@link
   * com.linkedin.datahub.graphql.generated.AccessTokenMetadata} type.
   */
  private void configureAccessAccessTokenMetadataResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "AccessToken",
        typeWiring ->
            typeWiring.dataFetcher(
                "metadata",
                new LoadableTypeResolver<>(
                    accessTokenMetadataType,
                    (env) -> ((AccessToken) env.getSource()).getMetadata().getUrn())));
    builder.type(
        "ListAccessTokenResult",
        typeWiring ->
            typeWiring.dataFetcher(
                "tokens",
                new LoadableTypeBatchResolver<>(
                    accessTokenMetadataType,
                    (env) ->
                        ((ListAccessTokenResult) env.getSource())
                            .getTokens().stream()
                                .map(AccessTokenMetadata::getUrn)
                                .collect(Collectors.toList()))));
  }

  private void configureGlossaryTermResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "GlossaryTerm",
        typeWiring ->
            typeWiring
                .dataFetcher("schemaMetadata", new AspectResolver())
                .dataFetcher("parentNodes", new ParentNodesResolver(entityClient))
                .dataFetcher("privileges", new EntityPrivilegesResolver(entityClient))
                .dataFetcher(
                    "aspects", new WeaklyTypedAspectsResolver(entityClient, entityRegistry))
                .dataFetcher("exists", new EntityExistsResolver(entityService)));
  }

  private void configureGlossaryNodeResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "GlossaryNode",
        typeWiring ->
            typeWiring
                .dataFetcher("parentNodes", new ParentNodesResolver(entityClient))
                .dataFetcher("privileges", new EntityPrivilegesResolver(entityClient))
                .dataFetcher("exists", new EntityExistsResolver(entityService))
                .dataFetcher(
                    "aspects", new WeaklyTypedAspectsResolver(entityClient, entityRegistry)));
  }

  private void configureSchemaFieldResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "SchemaFieldEntity",
        typeWiring ->
            typeWiring.dataFetcher(
                "parent",
                new EntityTypeResolver(
                    entityTypes, (env) -> ((SchemaFieldEntity) env.getSource()).getParent())));
  }

  private void configureEntityPathResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "EntityPath",
        typeWiring ->
            typeWiring.dataFetcher(
                "path",
                new BatchGetEntitiesResolver(
                    entityTypes, (env) -> ((EntityPath) env.getSource()).getPath())));
  }

  private void configureResolvedAuditStampResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "ResolvedAuditStamp",
        typeWiring ->
            typeWiring.dataFetcher(
                "actor",
                new LoadableTypeResolver<>(
                    corpUserType,
                    (env) -> ((ResolvedAuditStamp) env.getSource()).getActor().getUrn())));
  }

  /**
   * Configures resolvers responsible for resolving the {@link
   * com.linkedin.datahub.graphql.generated.CorpUser} type.
   */
  private void configureCorpUserResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "CorpUser",
        typeWiring ->
            typeWiring
                .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient))
                .dataFetcher("privileges", new EntityPrivilegesResolver(entityClient))
                .dataFetcher(
                    "aspects", new WeaklyTypedAspectsResolver(entityClient, entityRegistry))
                .dataFetcher("exists", new EntityExistsResolver(entityService)));
    builder.type(
        "CorpUserInfo",
        typeWiring ->
            typeWiring.dataFetcher(
                "manager",
                new LoadableTypeResolver<>(
                    corpUserType,
                    (env) -> ((CorpUserInfo) env.getSource()).getManager().getUrn())));
    builder.type(
        "CorpUserEditableProperties",
        typeWiring ->
            typeWiring.dataFetcher(
                "platforms",
                new LoadableTypeBatchResolver<>(
                    dataPlatformType,
                    (env) ->
                        ((CorpUserEditableProperties) env.getSource())
                            .getPlatforms().stream()
                                .map(DataPlatform::getUrn)
                                .collect(Collectors.toList()))));
  }

  /**
   * Configures resolvers responsible for resolving the {@link
   * com.linkedin.datahub.graphql.generated.CorpGroup} type.
   */
  private void configureCorpGroupResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "CorpGroup",
        typeWiring ->
            typeWiring
                .dataFetcher(
                    "relationships",
                    new EntityRelationshipsResultResolver(graphClient, entityService))
                .dataFetcher("privileges", new EntityPrivilegesResolver(entityClient))
                .dataFetcher(
                    "aspects", new WeaklyTypedAspectsResolver(entityClient, entityRegistry))
                .dataFetcher("exists", new EntityExistsResolver(entityService)));
    builder
        .type(
            "CorpGroupInfo",
            typeWiring ->
                typeWiring
                    .dataFetcher(
                        "admins",
                        new LoadableTypeBatchResolver<>(
                            corpUserType,
                            (env) ->
                                ((CorpGroupInfo) env.getSource())
                                    .getAdmins().stream()
                                        .map(CorpUser::getUrn)
                                        .collect(Collectors.toList())))
                    .dataFetcher(
                        "members",
                        new LoadableTypeBatchResolver<>(
                            corpUserType,
                            (env) ->
                                ((CorpGroupInfo) env.getSource())
                                    .getMembers().stream()
                                        .map(CorpUser::getUrn)
                                        .collect(Collectors.toList()))))
        .type(
            "ListGroupsResult",
            typeWiring ->
                typeWiring.dataFetcher(
                    "groups",
                    new LoadableTypeBatchResolver<>(
                        corpGroupType,
                        (env) ->
                            ((ListGroupsResult) env.getSource())
                                .getGroups().stream()
                                    .map(CorpGroup::getUrn)
                                    .collect(Collectors.toList()))));
  }

  private void configureTagAssociationResolver(final RuntimeWiring.Builder builder) {
    builder.type(
        "Tag",
        typeWiring ->
            typeWiring
                .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient))
                .dataFetcher(
                    "aspects", new WeaklyTypedAspectsResolver(entityClient, entityRegistry)));
    builder.type(
        "TagAssociation",
        typeWiring ->
            typeWiring.dataFetcher(
                "tag",
                new LoadableTypeResolver<>(
                    tagType,
                    (env) ->
                        ((com.linkedin.datahub.graphql.generated.TagAssociation) env.getSource())
                            .getTag()
                            .getUrn())));
  }

  private void configureGlossaryTermAssociationResolver(final RuntimeWiring.Builder builder) {
    builder.type(
        "GlossaryTermAssociation",
        typeWiring ->
            typeWiring.dataFetcher(
                "term",
                new LoadableTypeResolver<>(
                    glossaryTermType,
                    (env) -> ((GlossaryTermAssociation) env.getSource()).getTerm().getUrn())));
  }

  /**
   * Configures resolvers responsible for resolving the {@link
   * com.linkedin.datahub.graphql.generated.Notebook} type.
   */
  private void configureNotebookResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "Notebook",
        typeWiring ->
            typeWiring
                .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient))
                .dataFetcher(
                    "aspects", new WeaklyTypedAspectsResolver(entityClient, entityRegistry))
                .dataFetcher("browsePaths", new EntityBrowsePathsResolver(this.notebookType))
                .dataFetcher(
                    "platform",
                    new LoadableTypeResolver<>(
                        dataPlatformType,
                        (env) -> ((Notebook) env.getSource()).getPlatform().getUrn()))
                .dataFetcher("exists", new EntityExistsResolver(entityService))
                .dataFetcher(
                    "dataPlatformInstance",
                    new LoadableTypeResolver<>(
                        dataPlatformInstanceType,
                        (env) -> {
                          final Notebook notebook = env.getSource();
                          return notebook.getDataPlatformInstance() != null
                              ? notebook.getDataPlatformInstance().getUrn()
                              : null;
                        })));
  }

  /**
   * Configures resolvers responsible for resolving the {@link
   * com.linkedin.datahub.graphql.generated.Dashboard} type.
   */
  private void configureDashboardResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "Dashboard",
        typeWiring ->
            typeWiring
                .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient))
                .dataFetcher("browsePaths", new EntityBrowsePathsResolver(this.dashboardType))
                .dataFetcher(
                    "lineage",
                    new EntityLineageResultResolver(
                        siblingGraphService, restrictedService, this.authorizationConfiguration))
                .dataFetcher(
                    "aspects", new WeaklyTypedAspectsResolver(entityClient, entityRegistry))
                .dataFetcher(
                    "platform",
                    new LoadableTypeResolver<>(
                        dataPlatformType,
                        (env) -> ((Dashboard) env.getSource()).getPlatform().getUrn()))
                .dataFetcher(
                    "dataPlatformInstance",
                    new LoadableTypeResolver<>(
                        dataPlatformInstanceType,
                        (env) -> {
                          final Dashboard dashboard = env.getSource();
                          return dashboard.getDataPlatformInstance() != null
                              ? dashboard.getDataPlatformInstance().getUrn()
                              : null;
                        }))
                .dataFetcher(
                    "container",
                    new LoadableTypeResolver<>(
                        containerType,
                        (env) -> {
                          final Dashboard dashboard = env.getSource();
                          return dashboard.getContainer() != null
                              ? dashboard.getContainer().getUrn()
                              : null;
                        }))
                .dataFetcher("parentContainers", new ParentContainersResolver(entityClient))
                .dataFetcher("usageStats", new DashboardUsageStatsResolver(timeseriesAspectService))
                .dataFetcher(
                    "statsSummary", new DashboardStatsSummaryResolver(timeseriesAspectService))
                .dataFetcher("privileges", new EntityPrivilegesResolver(entityClient))
                .dataFetcher("exists", new EntityExistsResolver(entityService))
                .dataFetcher(
                    "health",
                    new EntityHealthResolver(
                        entityClient,
                        graphClient,
                        timeseriesAspectService,
                        new EntityHealthResolver.Config(false, true))));
    builder.type(
        "DashboardInfo",
        typeWiring ->
            typeWiring.dataFetcher(
                "charts",
                new LoadableTypeBatchResolver<>(
                    chartType,
                    (env) ->
                        ((DashboardInfo) env.getSource())
                            .getCharts().stream()
                                .map(Chart::getUrn)
                                .collect(Collectors.toList()))));
    builder.type(
        "DashboardUserUsageCounts",
        typeWiring ->
            typeWiring.dataFetcher(
                "user",
                new LoadableTypeResolver<>(
                    corpUserType,
                    (env) -> ((DashboardUserUsageCounts) env.getSource()).getUser().getUrn())));
    builder.type(
        "DashboardStatsSummary",
        typeWiring ->
            typeWiring.dataFetcher(
                "topUsersLast30Days",
                new LoadableTypeBatchResolver<>(
                    corpUserType,
                    (env) -> {
                      DashboardStatsSummary summary = ((DashboardStatsSummary) env.getSource());
                      return summary.getTopUsersLast30Days() != null
                          ? summary.getTopUsersLast30Days().stream()
                              .map(CorpUser::getUrn)
                              .collect(Collectors.toList())
                          : null;
                    })));
  }

  private void configureStructuredPropertyResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "StructuredPropertyDefinition",
        typeWiring ->
            typeWiring
                .dataFetcher(
                    "valueType",
                    new LoadableTypeResolver<>(
                        dataTypeType,
                        (env) ->
                            ((StructuredPropertyDefinition) env.getSource())
                                .getValueType()
                                .getUrn()))
                .dataFetcher(
                    "entityTypes",
                    new LoadableTypeBatchResolver<>(
                        entityTypeType,
                        (env) ->
                            ((StructuredPropertyDefinition) env.getSource())
                                .getEntityTypes().stream()
                                    .map(entityTypeType.getKeyProvider())
                                    .collect(Collectors.toList()))));
    builder.type(
        "TypeQualifier",
        typeWiring ->
            typeWiring.dataFetcher(
                "allowedTypes",
                new LoadableTypeBatchResolver<>(
                    entityTypeType,
                    (env) ->
                        ((TypeQualifier) env.getSource())
                            .getAllowedTypes().stream()
                                .map(entityTypeType.getKeyProvider())
                                .collect(Collectors.toList()))));
    builder.type(
        "StructuredPropertyEntity",
        typeWiring -> typeWiring.dataFetcher("exists", new EntityExistsResolver(entityService)));
  }

  /**
   * Configures resolvers responsible for resolving the {@link
   * com.linkedin.datahub.graphql.generated.Chart} type.
   */
  private void configureChartResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "Chart",
        typeWiring ->
            typeWiring
                .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient))
                .dataFetcher("browsePaths", new EntityBrowsePathsResolver(this.chartType))
                .dataFetcher(
                    "aspects", new WeaklyTypedAspectsResolver(entityClient, entityRegistry))
                .dataFetcher(
                    "lineage",
                    new EntityLineageResultResolver(
                        siblingGraphService, restrictedService, this.authorizationConfiguration))
                .dataFetcher(
                    "platform",
                    new LoadableTypeResolver<>(
                        dataPlatformType,
                        (env) -> ((Chart) env.getSource()).getPlatform().getUrn()))
                .dataFetcher(
                    "dataPlatformInstance",
                    new LoadableTypeResolver<>(
                        dataPlatformInstanceType,
                        (env) -> {
                          final Chart chart = env.getSource();
                          return chart.getDataPlatformInstance() != null
                              ? chart.getDataPlatformInstance().getUrn()
                              : null;
                        }))
                .dataFetcher(
                    "container",
                    new LoadableTypeResolver<>(
                        containerType,
                        (env) -> {
                          final Chart chart = env.getSource();
                          return chart.getContainer() != null
                              ? chart.getContainer().getUrn()
                              : null;
                        }))
                .dataFetcher("parentContainers", new ParentContainersResolver(entityClient))
                .dataFetcher(
                    "statsSummary", new ChartStatsSummaryResolver(this.timeseriesAspectService))
                .dataFetcher("privileges", new EntityPrivilegesResolver(entityClient))
                .dataFetcher("exists", new EntityExistsResolver(entityService))
                .dataFetcher(
                    "health",
                    new EntityHealthResolver(
                        entityClient,
                        graphClient,
                        timeseriesAspectService,
                        new EntityHealthResolver.Config(false, true))));
    builder.type(
        "ChartInfo",
        typeWiring ->
            typeWiring.dataFetcher(
                "inputs",
                new LoadableTypeBatchResolver<>(
                    datasetType,
                    (env) ->
                        ((ChartInfo) env.getSource())
                            .getInputs().stream()
                                .map(datasetType.getKeyProvider())
                                .collect(Collectors.toList()))));
  }

  /** Configures {@link graphql.schema.TypeResolver}s for any GQL 'union' or 'interface' types. */
  private void configureTypeResolvers(final RuntimeWiring.Builder builder) {
    builder
        .type(
            "Entity",
            typeWiring ->
                typeWiring.typeResolver(
                    new EntityInterfaceTypeResolver(
                        loadableTypes.stream()
                            .filter(graphType -> graphType instanceof EntityType)
                            .map(graphType -> (EntityType<?, ?>) graphType)
                            .collect(Collectors.toList()))))
        .type(
            "EntityWithRelationships",
            typeWiring ->
                typeWiring.typeResolver(
                    new EntityInterfaceTypeResolver(
                        loadableTypes.stream()
                            .filter(graphType -> graphType instanceof EntityType)
                            .map(graphType -> (EntityType<?, ?>) graphType)
                            .collect(Collectors.toList()))))
        .type(
            "BrowsableEntity",
            typeWiring ->
                typeWiring.typeResolver(
                    new EntityInterfaceTypeResolver(
                        browsableTypes.stream()
                            .map(graphType -> (EntityType<?, ?>) graphType)
                            .collect(Collectors.toList()))))
        .type(
            "OwnerType",
            typeWiring ->
                typeWiring.typeResolver(
                    new EntityInterfaceTypeResolver(
                        ownerTypes.stream()
                            .filter(graphType -> graphType instanceof EntityType)
                            .map(graphType -> (EntityType<?, ?>) graphType)
                            .collect(Collectors.toList()))))
        .type(
            "PlatformSchema",
            typeWiring -> typeWiring.typeResolver(new PlatformSchemaUnionTypeResolver()))
        .type(
            "HyperParameterValueType",
            typeWiring -> typeWiring.typeResolver(new HyperParameterValueTypeResolver()))
        .type("PropertyValue", typeWiring -> typeWiring.typeResolver(new PropertyValueResolver()))
        .type("ResolvedActor", typeWiring -> typeWiring.typeResolver(new ResolvedActorResolver()))
        .type("Aspect", typeWiring -> typeWiring.typeResolver(new AspectInterfaceTypeResolver()))
        .type(
            "TimeSeriesAspect",
            typeWiring -> typeWiring.typeResolver(new TimeSeriesAspectInterfaceTypeResolver()))
        .type("ResultsType", typeWiring -> typeWiring.typeResolver(new ResultsTypeResolver()))
        .type(
            "SupportsVersions",
            typeWiring ->
                typeWiring.typeResolver(
                    new EntityInterfaceTypeResolver(
                        loadableTypes.stream()
                            .map(graphType -> (EntityType<?, ?>) graphType)
                            .collect(Collectors.toList()))));
  }

  /** Configures custom type extensions leveraged within our GraphQL schema. */
  private void configureTypeExtensions(final RuntimeWiring.Builder builder) {
    builder.scalar(GraphQLLong);
  }

  /** Configures resolvers responsible for resolving the {@link ERModelRelationship} type. */
  private void configureERModelRelationshipResolvers(final RuntimeWiring.Builder builder) {
    builder
        .type(
            "ERModelRelationship",
            typeWiring ->
                typeWiring
                    .dataFetcher("privileges", new EntityPrivilegesResolver(entityClient))
                    .dataFetcher(
                        "relationships", new EntityRelationshipsResultResolver(graphClient)))
        .type(
            "ERModelRelationshipProperties",
            typeWiring ->
                typeWiring
                    .dataFetcher(
                        "source",
                        new LoadableTypeResolver<>(
                            datasetType,
                            (env) -> {
                              final ERModelRelationshipProperties erModelRelationshipProperties =
                                  env.getSource();
                              return erModelRelationshipProperties.getSource() != null
                                  ? erModelRelationshipProperties.getSource().getUrn()
                                  : null;
                            }))
                    .dataFetcher(
                        "destination",
                        new LoadableTypeResolver<>(
                            datasetType,
                            (env) -> {
                              final ERModelRelationshipProperties erModelRelationshipProperties =
                                  env.getSource();
                              return erModelRelationshipProperties.getDestination() != null
                                  ? erModelRelationshipProperties.getDestination().getUrn()
                                  : null;
                            })))
        .type(
            "Owner",
            typeWiring ->
                typeWiring.dataFetcher(
                    "owner",
                    new OwnerTypeResolver<>(
                        ownerTypes, (env) -> ((Owner) env.getSource()).getOwner())))
        .type(
            "InstitutionalMemoryMetadata",
            typeWiring ->
                typeWiring.dataFetcher(
                    "author",
                    new LoadableTypeResolver<>(
                        corpUserType,
                        (env) ->
                            ((InstitutionalMemoryMetadata) env.getSource()).getAuthor().getUrn())));
  }

  /**
   * Configures resolvers responsible for resolving the {@link
   * com.linkedin.datahub.graphql.generated.DataJob} type.
   */
  private void configureDataJobResolvers(final RuntimeWiring.Builder builder) {
    builder
        .type(
            "DataJob",
            typeWiring ->
                typeWiring
                    .dataFetcher(
                        "relationships", new EntityRelationshipsResultResolver(graphClient))
                    .dataFetcher("browsePaths", new EntityBrowsePathsResolver(this.dataJobType))
                    .dataFetcher(
                        "lineage",
                        new EntityLineageResultResolver(
                            siblingGraphService,
                            restrictedService,
                            this.authorizationConfiguration))
                    .dataFetcher(
                        "aspects", new WeaklyTypedAspectsResolver(entityClient, entityRegistry))
                    .dataFetcher(
                        "dataFlow",
                        new LoadableTypeResolver<>(
                            dataFlowType,
                            (env) -> {
                              final DataJob dataJob = env.getSource();
                              return dataJob.getDataFlow() != null
                                  ? dataJob.getDataFlow().getUrn()
                                  : null;
                            }))
                    .dataFetcher(
                        "dataPlatformInstance",
                        new LoadableTypeResolver<>(
                            dataPlatformInstanceType,
                            (env) -> {
                              final DataJob dataJob = env.getSource();
                              return dataJob.getDataPlatformInstance() != null
                                  ? dataJob.getDataPlatformInstance().getUrn()
                                  : null;
                            }))
                    .dataFetcher(
                        "container",
                        new LoadableTypeResolver<>(
                            containerType,
                            (env) -> {
                              final DataJob dataJob = env.getSource();
                              return dataJob.getContainer() != null
                                  ? dataJob.getContainer().getUrn()
                                  : null;
                            }))
                    .dataFetcher("parentContainers", new ParentContainersResolver(entityClient))
                    .dataFetcher("runs", new DataJobRunsResolver(entityClient))
                    .dataFetcher("privileges", new EntityPrivilegesResolver(entityClient))
                    .dataFetcher("exists", new EntityExistsResolver(entityService))
                    .dataFetcher(
                        "health",
                        new EntityHealthResolver(
                            entityClient,
                            graphClient,
                            timeseriesAspectService,
                            new EntityHealthResolver.Config(false, true))))
        .type(
            "DataJobInputOutput",
            typeWiring ->
                typeWiring
                    .dataFetcher(
                        "inputDatasets",
                        new LoadableTypeBatchResolver<>(
                            datasetType,
                            (env) ->
                                ((DataJobInputOutput) env.getSource())
                                    .getInputDatasets().stream()
                                        .map(datasetType.getKeyProvider())
                                        .collect(Collectors.toList())))
                    .dataFetcher(
                        "outputDatasets",
                        new LoadableTypeBatchResolver<>(
                            datasetType,
                            (env) ->
                                ((DataJobInputOutput) env.getSource())
                                    .getOutputDatasets().stream()
                                        .map(datasetType.getKeyProvider())
                                        .collect(Collectors.toList())))
                    .dataFetcher(
                        "inputDatajobs",
                        new LoadableTypeBatchResolver<>(
                            dataJobType,
                            (env) ->
                                ((DataJobInputOutput) env.getSource())
                                    .getInputDatajobs().stream()
                                        .map(DataJob::getUrn)
                                        .collect(Collectors.toList()))));
  }

  /**
   * Configures resolvers responsible for resolving the {@link
   * com.linkedin.datahub.graphql.generated.DataFlow} type.
   */
  private void configureDataFlowResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "DataFlow",
        typeWiring ->
            typeWiring
                .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient))
                .dataFetcher("browsePaths", new EntityBrowsePathsResolver(this.dataFlowType))
                .dataFetcher(
                    "lineage",
                    new EntityLineageResultResolver(
                        siblingGraphService, restrictedService, this.authorizationConfiguration))
                .dataFetcher(
                    "aspects", new WeaklyTypedAspectsResolver(entityClient, entityRegistry))
                .dataFetcher(
                    "platform",
                    new LoadableTypeResolver<>(
                        dataPlatformType,
                        (env) -> ((DataFlow) env.getSource()).getPlatform().getUrn()))
                .dataFetcher("exists", new EntityExistsResolver(entityService))
                .dataFetcher("privileges", new EntityPrivilegesResolver(entityClient))
                .dataFetcher(
                    "dataPlatformInstance",
                    new LoadableTypeResolver<>(
                        dataPlatformInstanceType,
                        (env) -> {
                          final DataFlow dataFlow = env.getSource();
                          return dataFlow.getDataPlatformInstance() != null
                              ? dataFlow.getDataPlatformInstance().getUrn()
                              : null;
                        }))
                .dataFetcher(
                    "container",
                    new LoadableTypeResolver<>(
                        containerType,
                        (env) -> {
                          final DataFlow dataFlow = env.getSource();
                          return dataFlow.getContainer() != null
                              ? dataFlow.getContainer().getUrn()
                              : null;
                        }))
                .dataFetcher("parentContainers", new ParentContainersResolver(entityClient))
                .dataFetcher(
                    "health",
                    new EntityHealthResolver(
                        entityClient,
                        graphClient,
                        timeseriesAspectService,
                        new EntityHealthResolver.Config(false, true))));
  }

  /**
   * Configures resolvers responsible for resolving the {@link
   * com.linkedin.datahub.graphql.generated.MLFeatureTable} type.
   */
  private void configureMLFeatureTableResolvers(final RuntimeWiring.Builder builder) {
    builder
        .type(
            "MLFeatureTable",
            typeWiring ->
                typeWiring
                    .dataFetcher(
                        "relationships", new EntityRelationshipsResultResolver(graphClient))
                    .dataFetcher(
                        "browsePaths", new EntityBrowsePathsResolver(this.mlFeatureTableType))
                    .dataFetcher(
                        "aspects", new WeaklyTypedAspectsResolver(entityClient, entityRegistry))
                    .dataFetcher(
                        "lineage",
                        new EntityLineageResultResolver(
                            siblingGraphService,
                            restrictedService,
                            this.authorizationConfiguration))
                    .dataFetcher("exists", new EntityExistsResolver(entityService))
                    .dataFetcher(
                        "platform",
                        new LoadableTypeResolver<>(
                            dataPlatformType,
                            (env) -> ((MLFeatureTable) env.getSource()).getPlatform().getUrn()))
                    .dataFetcher("privileges", new EntityPrivilegesResolver(entityClient))
                    .dataFetcher(
                        "dataPlatformInstance",
                        new LoadableTypeResolver<>(
                            dataPlatformInstanceType,
                            (env) -> {
                              final MLFeatureTable entity = env.getSource();
                              return entity.getDataPlatformInstance() != null
                                  ? entity.getDataPlatformInstance().getUrn()
                                  : null;
                            })))
        .type(
            "MLFeatureTableProperties",
            typeWiring ->
                typeWiring
                    .dataFetcher(
                        "mlFeatures",
                        new LoadableTypeBatchResolver<>(
                            mlFeatureType,
                            (env) ->
                                ((MLFeatureTableProperties) env.getSource()).getMlFeatures() != null
                                    ? ((MLFeatureTableProperties) env.getSource())
                                        .getMlFeatures().stream()
                                            .map(MLFeature::getUrn)
                                            .collect(Collectors.toList())
                                    : ImmutableList.of()))
                    .dataFetcher(
                        "mlPrimaryKeys",
                        new LoadableTypeBatchResolver<>(
                            mlPrimaryKeyType,
                            (env) ->
                                ((MLFeatureTableProperties) env.getSource()).getMlPrimaryKeys()
                                        != null
                                    ? ((MLFeatureTableProperties) env.getSource())
                                        .getMlPrimaryKeys().stream()
                                            .map(MLPrimaryKey::getUrn)
                                            .collect(Collectors.toList())
                                    : ImmutableList.of())))
        .type(
            "MLFeatureProperties",
            typeWiring ->
                typeWiring.dataFetcher(
                    "sources",
                    new LoadableTypeBatchResolver<>(
                        datasetType,
                        (env) -> {
                          if (((MLFeatureProperties) env.getSource()).getSources() == null) {
                            return Collections.emptyList();
                          }
                          return ((MLFeatureProperties) env.getSource())
                              .getSources().stream()
                                  .map(datasetType.getKeyProvider())
                                  .collect(Collectors.toList());
                        })))
        .type(
            "MLPrimaryKeyProperties",
            typeWiring ->
                typeWiring.dataFetcher(
                    "sources",
                    new LoadableTypeBatchResolver<>(
                        datasetType,
                        (env) -> {
                          if (((MLPrimaryKeyProperties) env.getSource()).getSources() == null) {
                            return Collections.emptyList();
                          }
                          return ((MLPrimaryKeyProperties) env.getSource())
                              .getSources().stream()
                                  .map(datasetType.getKeyProvider())
                                  .collect(Collectors.toList());
                        })))
        .type(
            "MLModel",
            typeWiring ->
                typeWiring
                    .dataFetcher(
                        "relationships", new EntityRelationshipsResultResolver(graphClient))
                    .dataFetcher("browsePaths", new EntityBrowsePathsResolver(this.mlModelType))
                    .dataFetcher(
                        "lineage",
                        new EntityLineageResultResolver(
                            siblingGraphService,
                            restrictedService,
                            this.authorizationConfiguration))
                    .dataFetcher("exists", new EntityExistsResolver(entityService))
                    .dataFetcher(
                        "aspects", new WeaklyTypedAspectsResolver(entityClient, entityRegistry))
                    .dataFetcher(
                        "platform",
                        new LoadableTypeResolver<>(
                            dataPlatformType,
                            (env) -> ((MLModel) env.getSource()).getPlatform().getUrn()))
                    .dataFetcher("privileges", new EntityPrivilegesResolver(entityClient))
                    .dataFetcher(
                        "dataPlatformInstance",
                        new LoadableTypeResolver<>(
                            dataPlatformInstanceType,
                            (env) -> {
                              final MLModel mlModel = env.getSource();
                              return mlModel.getDataPlatformInstance() != null
                                  ? mlModel.getDataPlatformInstance().getUrn()
                                  : null;
                            })))
        .type(
            "MLModelProperties",
            typeWiring ->
                typeWiring.dataFetcher(
                    "groups",
                    new LoadableTypeBatchResolver<>(
                        mlModelGroupType,
                        (env) -> {
                          MLModelProperties properties = env.getSource();
                          if (properties.getGroups() != null) {
                            return properties.getGroups().stream()
                                .map(MLModelGroup::getUrn)
                                .collect(Collectors.toList());
                          }
                          return Collections.emptyList();
                        })))
        .type(
            "MLModelGroup",
            typeWiring ->
                typeWiring
                    .dataFetcher(
                        "relationships", new EntityRelationshipsResultResolver(graphClient))
                    .dataFetcher(
                        "browsePaths", new EntityBrowsePathsResolver(this.mlModelGroupType))
                    .dataFetcher(
                        "aspects", new WeaklyTypedAspectsResolver(entityClient, entityRegistry))
                    .dataFetcher(
                        "lineage",
                        new EntityLineageResultResolver(
                            siblingGraphService,
                            restrictedService,
                            this.authorizationConfiguration))
                    .dataFetcher(
                        "platform",
                        new LoadableTypeResolver<>(
                            dataPlatformType,
                            (env) -> ((MLModelGroup) env.getSource()).getPlatform().getUrn()))
                    .dataFetcher("exists", new EntityExistsResolver(entityService))
                    .dataFetcher("privileges", new EntityPrivilegesResolver(entityClient))
                    .dataFetcher(
                        "dataPlatformInstance",
                        new LoadableTypeResolver<>(
                            dataPlatformInstanceType,
                            (env) -> {
                              final MLModelGroup entity = env.getSource();
                              return entity.getDataPlatformInstance() != null
                                  ? entity.getDataPlatformInstance().getUrn()
                                  : null;
                            })))
        .type(
            "MLFeature",
            typeWiring ->
                typeWiring
                    .dataFetcher(
                        "relationships", new EntityRelationshipsResultResolver(graphClient))
                    .dataFetcher(
                        "lineage",
                        new EntityLineageResultResolver(
                            siblingGraphService,
                            restrictedService,
                            this.authorizationConfiguration))
                    .dataFetcher(
                        "aspects", new WeaklyTypedAspectsResolver(entityClient, entityRegistry))
                    .dataFetcher("exists", new EntityExistsResolver(entityService))
                    .dataFetcher("privileges", new EntityPrivilegesResolver(entityClient))
                    .dataFetcher(
                        "dataPlatformInstance",
                        new LoadableTypeResolver<>(
                            dataPlatformInstanceType,
                            (env) -> {
                              final MLFeature entity = env.getSource();
                              return entity.getDataPlatformInstance() != null
                                  ? entity.getDataPlatformInstance().getUrn()
                                  : null;
                            })))
        .type(
            "MLPrimaryKey",
            typeWiring ->
                typeWiring
                    .dataFetcher(
                        "relationships", new EntityRelationshipsResultResolver(graphClient))
                    .dataFetcher("privileges", new EntityPrivilegesResolver(entityClient))
                    .dataFetcher(
                        "lineage",
                        new EntityLineageResultResolver(
                            siblingGraphService,
                            restrictedService,
                            this.authorizationConfiguration))
                    .dataFetcher(
                        "aspects", new WeaklyTypedAspectsResolver(entityClient, entityRegistry))
                    .dataFetcher("exists", new EntityExistsResolver(entityService))
                    .dataFetcher(
                        "dataPlatformInstance",
                        new LoadableTypeResolver<>(
                            dataPlatformInstanceType,
                            (env) -> {
                              final MLPrimaryKey entity = env.getSource();
                              return entity.getDataPlatformInstance() != null
                                  ? entity.getDataPlatformInstance().getUrn()
                                  : null;
                            })));
  }

  private void configureGlossaryRelationshipResolvers(final RuntimeWiring.Builder builder) {
    builder
        .type(
            "GlossaryTerm",
            typeWiring ->
                typeWiring.dataFetcher(
                    "relationships", new EntityRelationshipsResultResolver(graphClient)))
        .type(
            "GlossaryNode",
            typeWiring ->
                typeWiring.dataFetcher(
                    "relationships", new EntityRelationshipsResultResolver(graphClient)));
  }

  private void configureDomainResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "Domain",
        typeWiring ->
            typeWiring
                .dataFetcher("entities", new DomainEntitiesResolver(this.entityClient))
                .dataFetcher("parentDomains", new ParentDomainsResolver(this.entityClient))
                .dataFetcher("privileges", new EntityPrivilegesResolver(entityClient))
                .dataFetcher(
                    "aspects", new WeaklyTypedAspectsResolver(entityClient, entityRegistry))
                .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient)));
    builder.type(
        "DomainAssociation",
        typeWiring ->
            typeWiring.dataFetcher(
                "domain",
                new LoadableTypeResolver<>(
                    domainType,
                    (env) ->
                        ((com.linkedin.datahub.graphql.generated.DomainAssociation) env.getSource())
                            .getDomain()
                            .getUrn())));
  }

  private void configureFormResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "FormAssociation",
        typeWiring ->
            typeWiring.dataFetcher(
                "form",
                new LoadableTypeResolver<>(
                    formType,
                    (env) ->
                        ((com.linkedin.datahub.graphql.generated.FormAssociation) env.getSource())
                            .getForm()
                            .getUrn())));
    builder.type(
        "StructuredPropertyParams",
        typeWiring ->
            typeWiring.dataFetcher(
                "structuredProperty",
                new LoadableTypeResolver<>(
                    structuredPropertyType,
                    (env) ->
                        ((StructuredPropertyParams) env.getSource())
                            .getStructuredProperty()
                            .getUrn())));
    builder.type(
        "FormActorAssignment",
        typeWiring ->
            typeWiring
                .dataFetcher(
                    "users",
                    new LoadableTypeBatchResolver<>(
                        corpUserType,
                        (env) -> {
                          final FormActorAssignment actors = env.getSource();
                          return actors.getUsers() != null
                              ? actors.getUsers().stream()
                                  .map(CorpUser::getUrn)
                                  .collect(Collectors.toList())
                              : null;
                        }))
                .dataFetcher(
                    "groups",
                    new LoadableTypeBatchResolver<>(
                        corpGroupType,
                        (env) -> {
                          final FormActorAssignment actors = env.getSource();
                          return actors.getGroups() != null
                              ? actors.getGroups().stream()
                                  .map(CorpGroup::getUrn)
                                  .collect(Collectors.toList())
                              : null;
                        }))
                .dataFetcher("isAssignedToMe", new IsFormAssignedToMeResolver(groupService)));
  }

  private void configureDataProductResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "DataProduct",
        typeWiring ->
            typeWiring
                .dataFetcher("entities", new ListDataProductAssetsResolver(this.entityClient))
                .dataFetcher("privileges", new EntityPrivilegesResolver(entityClient))
                .dataFetcher(
                    "aspects", new WeaklyTypedAspectsResolver(entityClient, entityRegistry))
                .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient)));
  }

  private void configureAssertionResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "Assertion",
        typeWiring ->
            typeWiring
                .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient))
                .dataFetcher(
                    "platform",
                    new LoadableTypeResolver<>(
                        dataPlatformType,
                        (env) -> ((Assertion) env.getSource()).getPlatform().getUrn()))
                .dataFetcher(
                    "dataPlatformInstance",
                    new LoadableTypeResolver<>(
                        dataPlatformInstanceType,
                        (env) -> {
                          final Assertion assertion = env.getSource();
                          return assertion.getDataPlatformInstance() != null
                              ? assertion.getDataPlatformInstance().getUrn()
                              : null;
                        }))
                .dataFetcher("runEvents", new AssertionRunEventResolver(entityClient))
                .dataFetcher(
                    "aspects", new WeaklyTypedAspectsResolver(entityClient, entityRegistry)));
  }

  private void configureContractResolvers(final RuntimeWiring.Builder builder) {
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
                    getAssertionType(),
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
                    getAssertionType(),
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
                    getAssertionType(),
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

  private void configurePolicyResolvers(final RuntimeWiring.Builder builder) {
    // Register resolvers for "resolvedUsers" and "resolvedGroups" field of the
    // Policy type.
    builder.type(
        "ActorFilter",
        typeWiring ->
            typeWiring
                .dataFetcher(
                    "resolvedUsers",
                    new LoadableTypeBatchResolver<>(
                        corpUserType,
                        (env) -> {
                          final ActorFilter filter = env.getSource();
                          return filter.getUsers();
                        }))
                .dataFetcher(
                    "resolvedGroups",
                    new LoadableTypeBatchResolver<>(
                        corpGroupType,
                        (env) -> {
                          final ActorFilter filter = env.getSource();
                          return filter.getGroups();
                        }))
                .dataFetcher(
                    "resolvedRoles",
                    new LoadableTypeBatchResolver<>(
                        dataHubRoleType,
                        (env) -> {
                          final ActorFilter filter = env.getSource();
                          return filter.getRoles();
                        }))
                .dataFetcher(
                    "resolvedOwnershipTypes",
                    new LoadableTypeBatchResolver<>(
                        ownershipType,
                        (env) -> {
                          final ActorFilter filter = env.getSource();
                          return filter.getResourceOwnersTypes();
                        })));
  }

  private void configureDataHubRoleResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "DataHubRole",
        typeWiring ->
            typeWiring.dataFetcher(
                "relationships", new EntityRelationshipsResultResolver(graphClient)));
  }

  private void configureViewResolvers(final RuntimeWiring.Builder builder) {
    builder
        .type(
            "DataHubView",
            typeWiring ->
                typeWiring.dataFetcher(
                    "relationships", new EntityRelationshipsResultResolver(graphClient)))
        .type(
            "ListViewsResult",
            typeWiring ->
                typeWiring.dataFetcher(
                    "views",
                    new LoadableTypeBatchResolver<>(
                        dataHubViewType,
                        (env) ->
                            ((ListViewsResult) env.getSource())
                                .getViews().stream()
                                    .map(DataHubView::getUrn)
                                    .collect(Collectors.toList()))))
        .type(
            "CorpUserViewsSettings",
            typeWiring ->
                typeWiring.dataFetcher(
                    "defaultView",
                    new LoadableTypeResolver<>(
                        dataHubViewType,
                        (env) -> {
                          final CorpUserViewsSettings settings = env.getSource();
                          if (settings.getDefaultView() != null) {
                            return settings.getDefaultView().getUrn();
                          }
                          return null;
                        })));
  }

  private void configureQueryEntityResolvers(final RuntimeWiring.Builder builder) {
    builder
        .type(
            "QueryEntity",
            typeWiring ->
                typeWiring
                    .dataFetcher(
                        "relationships", new EntityRelationshipsResultResolver(graphClient))
                    .dataFetcher(
                        "platform",
                        new LoadableTypeResolver<>(
                            dataPlatformType,
                            (env) -> {
                              final QueryEntity query = env.getSource();
                              return query.getPlatform() != null
                                  ? query.getPlatform().getUrn()
                                  : null;
                            })))
        .type(
            "QueryProperties",
            typeWiring ->
                typeWiring.dataFetcher(
                    "origin",
                    new EntityTypeResolver(
                        entityTypes, (env) -> ((QueryProperties) env.getSource()).getOrigin())))
        .type(
            "ListQueriesResult",
            typeWiring ->
                typeWiring.dataFetcher(
                    "queries",
                    new LoadableTypeBatchResolver<>(
                        queryType,
                        (env) ->
                            ((ListQueriesResult) env.getSource())
                                .getQueries().stream()
                                    .map(QueryEntity::getUrn)
                                    .collect(Collectors.toList()))))
        .type(
            "QuerySubject",
            typeWiring ->
                typeWiring.dataFetcher(
                    "dataset",
                    new LoadableTypeResolver<>(
                        datasetType,
                        (env) -> ((QuerySubject) env.getSource()).getDataset().getUrn())));
  }

  private void configureOwnershipTypeResolver(final RuntimeWiring.Builder builder) {
    builder
        .type(
            "OwnershipTypeEntity",
            typeWiring ->
                typeWiring.dataFetcher(
                    "relationships", new EntityRelationshipsResultResolver(graphClient)))
        .type(
            "ListOwnershipTypesResult",
            typeWiring ->
                typeWiring.dataFetcher(
                    "ownershipTypes",
                    new LoadableTypeBatchResolver<>(
                        ownershipType,
                        (env) ->
                            ((ListOwnershipTypesResult) env.getSource())
                                .getOwnershipTypes().stream()
                                    .map(OwnershipTypeEntity::getUrn)
                                    .collect(Collectors.toList()))));
  }

  private void configureDataProcessInstanceResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "DataProcessInstance",
        typeWiring ->
            typeWiring
                .dataFetcher(
                    "dataPlatformInstance",
                    new LoadableTypeResolver<>(
                        dataPlatformInstanceType,
                        (env) -> {
                          final DataProcessInstance dataProcessInstance = env.getSource();
                          return dataProcessInstance.getDataPlatformInstance() != null
                              ? dataProcessInstance.getDataPlatformInstance().getUrn()
                              : null;
                        }))
                .dataFetcher("parentContainers", new ParentContainersResolver(entityClient))
                .dataFetcher(
                    "container",
                    new LoadableTypeResolver<>(
                        containerType,
                        (env) -> {
                          final DataProcessInstance dpi = env.getSource();
                          return dpi.getContainer() != null ? dpi.getContainer().getUrn() : null;
                        }))
                .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient))
                .dataFetcher(
                    "lineage",
                    new EntityLineageResultResolver(
                        siblingGraphService, restrictedService, this.authorizationConfiguration))
                .dataFetcher(
                    "state",
                    new TimeSeriesAspectResolver(
                        this.entityClient,
                        "dataProcessInstance",
                        DATA_PROCESS_INSTANCE_RUN_EVENT_ASPECT_NAME,
                        DataProcessInstanceRunEventMapper::map)));
  }

  private void configureTestResultResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "TestResult",
        typeWiring ->
            typeWiring.dataFetcher(
                "test",
                new LoadableTypeResolver<>(
                    testType,
                    (env) -> {
                      final TestResult testResult = env.getSource();
                      return testResult.getTest() != null ? testResult.getTest().getUrn() : null;
                    })));
  }

  private static <T, K> DataLoader<K, DataFetcherResult<T>> createDataLoader(
      final LoadableType<T, K> graphType, final QueryContext queryContext) {
    BatchLoaderContextProvider contextProvider = () -> queryContext;
    DataLoaderOptions loaderOptions =
        DataLoaderOptions.newOptions().setBatchLoaderContextProvider(contextProvider);
    return DataLoader.newDataLoader(
        (keys, context) ->
            GraphQLConcurrencyUtils.supplyAsync(
                () -> {
                  try {
                    log.debug(
                        String.format(
                            "Batch loading entities of type: %s, keys: %s",
                            graphType.name(), keys));
                    return graphType.batchLoad(keys, context.getContext());
                  } catch (Exception e) {
                    log.error(
                        String.format(
                                "Failed to load Entities of type: %s, keys: %s",
                                graphType.name(), keys)
                            + " "
                            + e.getMessage());
                    throw new RuntimeException(
                        String.format("Failed to retrieve entities of type %s", graphType.name()),
                        e);
                  }
                },
                graphType.getClass().getSimpleName(),
                "batchLoad"),
        loaderOptions);
  }

  private void configureIngestionSourceResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "IngestionSource",
        typeWiring ->
            typeWiring
                .dataFetcher(
                    "executions", new IngestionSourceExecutionRequestsResolver(entityClient))
                .dataFetcher(
                    "platform",
                    new LoadableTypeResolver<>(
                        dataPlatformType,
                        (env) -> {
                          final IngestionSource ingestionSource = env.getSource();
                          return ingestionSource.getPlatform() != null
                              ? ingestionSource.getPlatform().getUrn()
                              : null;
                        })));
  }

  private void configureIncidentResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "Incident",
        typeWiring ->
            typeWiring.dataFetcher(
                "relationships", new EntityRelationshipsResultResolver(graphClient)));
    builder.type(
        "IncidentSource",
        typeWiring ->
            typeWiring.dataFetcher(
                "source",
                new LoadableTypeResolver<>(
                    this.assertionType,
                    (env) -> {
                      final IncidentSource incidentSource = env.getSource();
                      return incidentSource.getSource() != null
                          ? incidentSource.getSource().getUrn()
                          : null;
                    })));

    // Add incidents attribute to all entities that support it
    final List<String> entitiesWithIncidents =
        ImmutableList.of("Dataset", "DataJob", "DataFlow", "Dashboard", "Chart");
    for (String entity : entitiesWithIncidents) {
      builder.type(
          entity,
          typeWiring ->
              typeWiring.dataFetcher("incidents", new EntityIncidentsResolver(entityClient)));
    }
  }

  private void configureRestrictedResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "Restricted",
        typeWiring ->
            typeWiring
                .dataFetcher(
                    "lineage",
                    new EntityLineageResultResolver(
                        siblingGraphService, restrictedService, this.authorizationConfiguration))
                .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient)));
  }

  private void configureRoleResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "Role",
        typeWiring -> typeWiring.dataFetcher("isAssignedToMe", new IsAssignedToMeResolver()));
  }

  private void configureBusinessAttributeResolver(final RuntimeWiring.Builder builder) {
    builder
        .type(
            "BusinessAttribute",
            typeWiring ->
                typeWiring
                    .dataFetcher("exists", new EntityExistsResolver(entityService))
                    .dataFetcher("privileges", new EntityPrivilegesResolver(entityClient)))
        .type(
            "ListBusinessAttributesResult",
            typeWiring ->
                typeWiring.dataFetcher(
                    "businessAttributes",
                    new LoadableTypeBatchResolver<>(
                        businessAttributeType,
                        (env) ->
                            ((ListBusinessAttributesResult) env.getSource())
                                .getBusinessAttributes().stream()
                                    .map(BusinessAttribute::getUrn)
                                    .collect(Collectors.toList()))));
  }

  private void configureBusinessAttributeAssociationResolver(final RuntimeWiring.Builder builder) {
    builder.type(
        "BusinessAttributeAssociation",
        typeWiring ->
            typeWiring.dataFetcher(
                "businessAttribute",
                new LoadableTypeResolver<>(
                    businessAttributeType,
                    (env) ->
                        ((BusinessAttributeAssociation) env.getSource())
                            .getBusinessAttribute()
                            .getUrn())));
  }

  private void configureConnectionResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "Mutation",
        typeWiring ->
            typeWiring.dataFetcher(
                "upsertConnection",
                new UpsertConnectionResolver(connectionService, secretService)));
    builder.type(
        "Query",
        typeWiring -> typeWiring.dataFetcher("connection", getResolver(this.connectionType)));
    builder.type(
        "DataHubConnection",
        typeWiring ->
            typeWiring.dataFetcher(
                "platform",
                new LoadableTypeResolver<>(
                    this.dataPlatformType,
                    (env) -> {
                      final DataHubConnection connection = env.getSource();
                      return connection.getPlatform() != null
                          ? connection.getPlatform().getUrn()
                          : null;
                    })));
  }

  private void configureDeprecationResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "Deprecation",
        typeWiring ->
            typeWiring.dataFetcher(
                "actorEntity",
                new EntityTypeResolver(
                    entityTypes, (env) -> ((Deprecation) env.getSource()).getActorEntity())));
  }

  private void configureMetadataAttributionResolver(final RuntimeWiring.Builder builder) {
    builder.type(
        "MetadataAttribution",
        typeWiring ->
            typeWiring
                .dataFetcher(
                    "actor",
                    new EntityTypeResolver(
                        entityTypes, (env) -> ((MetadataAttribution) env.getSource()).getActor()))
                .dataFetcher(
                    "source",
                    new EntityTypeResolver(
                        entityTypes,
                        (env) -> ((MetadataAttribution) env.getSource()).getSource())));
  }

  private void configureVersionPropertiesResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "VersionProperties",
        typeWiring ->
            typeWiring.dataFetcher(
                "versionSet",
                new LoadableTypeResolver<>(
                    versionSetType,
                    (env) -> {
                      final VersionProperties versionProperties = env.getSource();
                      return versionProperties != null
                          ? versionProperties.getVersionSet().getUrn()
                          : null;
                    })));
  }

  private void configureVersionSetResolvers(final RuntimeWiring.Builder builder) {
    builder.type(
        "VersionSet",
        typeWiring ->
            typeWiring
                .dataFetcher(
                    "latestVersion",
                    new EntityTypeResolver(
                        entityTypes, (env) -> ((VersionSet) env.getSource()).getLatestVersion()))
                .dataFetcher(
                    "versionsSearch",
                    new VersionsSearchResolver(this.entityClient, this.viewService)));
  }
}
