import { FileUploadFailureType } from '@components/components/Editor/types';

import { EmbedLookupNotFoundReason } from '@app/embed/lookup/constants';
import { PersonaType } from '@app/homeV2/shared/types';
import { Direction } from '@app/lineage/types';
import { FilterMode } from '@app/search/utils/constants';
import { ActorType } from '@app/settings/personal/notifications/constants';

import {
    ActionRequestResult,
    ActionRequestType,
    ActionWorkflowCategory,
    AllowedValue,
    AssertionType,
    DataHubPageModuleType,
    DataHubViewType,
    EntityChangeType,
    EntityType,
    FormPromptType,
    FormType,
    LineageDirection,
    NotificationSinkType,
    OidcSettings,
    OwnerInput,
    PageTemplateSurfaceType,
    PropertyCardinality,
    PropertyValueInput,
    RecommendationRenderType,
    ResourceRefInput,
    ScenarioType,
    SearchBarApi,
    StructuredPropertyFilterStatus,
    SummaryElementType,
    UploadDownloadScenario,
} from '@types';

// NOTE: If we move this file, update metadata-ingestion/scripts/analyticseventsdocgen.sh with new path for auto generating docs

/**
 * Valid event types.
 */
export enum EventType {
    PageViewEvent,
    HomePageViewEvent,
    LogInEvent,
    LogOutEvent,
    SearchEvent,
    HomePageSearchEvent,
    SearchResultsViewEvent,
    SearchResultClickEvent,
    EntitySearchResultClickEvent,
    SearchFiltersClearAllEvent,
    SearchFiltersShowMoreEvent,
    BrowseResultClickEvent,
    HomePageBrowseResultClickEvent,
    BrowseV2ToggleSidebarEvent,
    BrowseV2ToggleNodeEvent,
    BrowseV2SelectNodeEvent,
    BrowseV2EntityLinkClickEvent,
    EntityViewEvent,
    EntitySectionViewEvent,
    EntityActionEvent,
    BatchEntityActionEvent,
    BatchProposalActionEvent,
    RecommendationImpressionEvent,
    RecommendationClickEvent,
    HomePageRecommendationClickEvent,
    HomePageExploreAllClickEvent,
    SearchBarExploreAllClickEvent,
    SearchResultsExploreAllClickEvent,
    SearchAcrossLineageEvent,
    VisualLineageViewEvent,
    VisualLineageExpandGraphEvent,
    SearchAcrossLineageResultsViewEvent,
    DownloadAsCsvEvent,
    SignUpEvent,
    ResetCredentialsEvent,
    CreateAccessTokenEvent,
    RevokeAccessTokenEvent,
    CreateGroupEvent,
    CreateInviteLinkEvent,
    CreateResetCredentialsLinkEvent,
    DeleteEntityEvent,
    SelectUserRoleEvent,
    SelectGroupRoleEvent,
    BatchSelectUserRoleEvent,
    CreatePolicyEvent,
    UpdatePolicyEvent,
    DeactivatePolicyEvent,
    ActivatePolicyEvent,
    ShowSimplifiedHomepageEvent,
    ShowStandardHomepageEvent,
    ShowV2ThemeEvent,
    RevertV2ThemeEvent,
    CreateGlossaryEntityEvent,
    CreateDomainEvent,
    MoveDomainEvent,
    IngestionTestConnectionEvent,
    IngestionExecutionResultViewedEvent,
    IngestionSourceConfigurationImpressionEvent,
    IngestionViewAllClickEvent,
    IngestionViewAllClickWarningEvent,
    CreateIngestionSourceEvent,
    UpdateIngestionSourceEvent,
    DeleteIngestionSourceEvent,
    ExecuteIngestionSourceEvent,
    SsoEvent,
    CreateViewEvent,
    UpdateViewEvent,
    SetGlobalDefaultViewEvent,
    SetUserDefaultViewEvent,
    ManuallyCreateLineageEvent,
    ManuallyDeleteLineageEvent,
    LineageGraphTimeRangeSelectionEvent,
    LineageTabTimeRangeSelectionEvent,
    CreateQueryEvent,
    UpdateQueryEvent,
    DeleteQueryEvent,
    SelectAutoCompleteOption,
    SelectQuickFilterEvent,
    DeselectQuickFilterEvent,
    EmbedProfileViewEvent,
    EmbedProfileViewInDataHubEvent,
    EmbedLookupNotFoundEvent,
    CreateBusinessAttributeEvent,
    CreateStructuredPropertyClickEvent,
    CreateStructuredPropertyEvent,
    EditStructuredPropertyEvent,
    DeleteStructuredPropertyEvent,
    ViewStructuredPropertyEvent,
    ApplyStructuredPropertyEvent,
    UpdateStructuredPropertyOnAssetEvent,
    RemoveStructuredPropertyEvent,
    ClickDocRequestCTA,
    CompleteDocRequestPrompt,
    CompleteVerification,
    OpenTaskCenter,
    GoToLogicalParentEvent,
    GoToPhysicalChildEvent,
    GoToLogicalParentColumnEvent,
    GoToPhysicalChildColumnEvent,
    // SaaS only events
    CreateTestEvent,
    UpdateTestEvent,
    DeleteTestEvent,
    CreateAssertionMonitorEvent,
    UpdateAssertionMonitorEvent,
    UpdateAssertionMetadataEvent,
    StartAssertionMonitorEvent,
    StopAssertionMonitorEvent,
    ViewAssertionNoteTabEvent,
    UpdateAssertionNoteEvent,
    SlackIntegrationSuccessEvent,
    SlackIntegrationErrorEvent,
    SubscriptionCreateSuccessEvent,
    SubscriptionCreateErrorEvent,
    SubscriptionUpdateSuccessEvent,
    SubscriptionUpdateErrorEvent,
    SubscriptionDeleteSuccessEvent,
    SubscriptionDeleteErrorEvent,
    NotificationSettingsSuccessEvent,
    NotificationSettingsErrorEvent,
    InboxPageViewEvent,
    IntroduceYourselfViewEvent,
    IntroduceYourselfSubmitEvent,
    IntroduceYourselfSkipEvent,
    SharedEntityEvent,
    UnsharedEntityEvent,
    ExpandLineageEvent,
    ContractLineageEvent,
    ShowHideLineageColumnsEvent,
    SearchLineageColumnsEvent,
    FilterLineageColumnsEvent,
    DrillDownLineageEvent,
    InferDocsClickEvent,
    AcceptInferredDocs,
    DeclineInferredDocs,
    CreateFormClickEvent,
    SaveFormAsDraftEvent,
    PublishFormEvent,
    UnpublishFormEvent,
    DeleteFormEvent,
    CreateQuestionEvent,
    EditQuestionEvent,
    SSOConfigurationEvent,
    ProposeStructuredPropertiesMutation,
    ProposeDomainMutation,
    ProposeOwnersMutation,
    LinkAssetVersionEvent,
    UnlinkAssetVersionEvent,
    ShowAllVersionsEvent,
    HomePageClick,
    SearchBarFilter,
    NavBarExpandCollapse,
    NavBarItemClick,
    FormByEntityNavigate,
    FormViewToggle,
    FormAnalyticsTabSelect,
    FormAnalyticsDownloadCsv,
    FormAnalyticsTabFilter,
    FilterStatsPage,
    FilterStatsChartLookBack,
    ClickCreateAssertion,
    ClickBulkCreateAssertion,
    BulkCreateAssertionSubmissionFailedEvent,
    BulkCreateAssertionSubmissionEvent,
    BulkCreateAssertionCompletedEvent,
    ClickUserProfile,
    ClickViewDocumentation,
    GiveAnomalyFeedback,
    UndoAnomalyFeedback,
    RetrainAsNewNormal,
    CreateActionWorkflowFormRequest,
    ReviewActionWorkflowFormRequest,
    BatchReviewActionWorkflowFormRequest,
    ClickProductUpdate,
    HomePageTemplateModuleCreate,
    HomePageTemplateModuleAdd,
    HomePageTemplateModuleUpdate,
    HomePageTemplateModuleDelete,
    HomePageTemplateModuleMove,
    HomePageTemplateModuleModalCreateOpen,
    HomePageTemplateModuleModalEditOpen,
    HomePageTemplateModuleModalCancel,
    HomePageTemplateGlobalTemplateEditingStart,
    HomePageTemplateGlobalTemplateEditingDone,
    HomePageTemplateResetToGlobalTemplate,
    HomePageTemplateModuleAssetClick,
    HomePageTemplateModuleViewAllClick,
    HomePageTemplateModuleExpandClick,
    HomePageTemplateModuleLinkClick,
    HomePageTemplateModuleAnnouncementDismiss,
    CreateActionEvent,
    UpdateActionEvent,
    DeleteActionEvent,
    DatasetHealthFilterEvent,
    DatasetHealthClickEvent,
    WelcomeToDataHubModalViewEvent,
    WelcomeToDataHubModalInteractEvent,
    WelcomeToDataHubModalExitEvent,
    WelcomeToDataHubModalClickViewDocumentationEvent,
    ProductTourButtonClickEvent,
    SetDeprecation,
    ClickInviteUsersCTAEvent,
    ClickCopyInviteLinkEvent,
    ClickInviteViaEmailEvent,
    ClickInviteRecommendedUserEvent,
    ClickBulkInviteRecommendedUsersEvent,
    ClickBulkDismissRecommendedUsersEvent,
    InviteUserErrorEvent,
    RefreshInviteLinkEvent,
    AssetPageAddSummaryElement,
    AssetPageRemoveSummaryElement,
    AssetPageReplaceSummaryElement,
    FileUploadAttemptEvent,
    FileUploadFailedEvent,
    FileUploadSucceededEvent,
    FileDownloadViewEvent,
}

/**
 * Base Interface for all React analytics events.
 */
interface BaseEvent {
    /** the urn of the actor who triggered this event */
    actorUrn?: string;
    /** timestamp for when this occurred */
    timestamp?: number;
    /** date for when this occurred */
    date?: string;
    /** the specs for the user's machine */
    userAgent?: string;
    /** unique ID given to a user's browser */
    browserId?: string;
    /** whether DataHub 2.0 UI is enabled or not at the time of this event */
    isThemeV2Enabled?: boolean;
    /** the persona urn for this user - groups users based on their titles */
    userPersona?: PersonaType;
    /** the selected title of this user ie. "Data Analyst" */
    userTitle?: string;
    /** the current server version when this event happened */
    serverVersion?: string;
}

/**
 * Viewed a page on the UI.
 */
export interface PageViewEvent extends BaseEvent {
    type: EventType.PageViewEvent;
    originPath: string;
}

/**
 * Viewed the Introduce Yourself page on the UI.
 */
export interface IntroduceYourselfViewEvent extends BaseEvent {
    type: EventType.IntroduceYourselfViewEvent;
}

/**
 * Submitted the "Introduce Yourself" page through the UI.
 */
export interface IntroduceYourselfSubmitEvent extends BaseEvent {
    type: EventType.IntroduceYourselfSubmitEvent;
    role: string;
    platformUrns: Array<string>;
}

/**
 * Skipped the "Introduce Yourself" page through the UI.
 */
export interface IntroduceYourselfSkipEvent extends BaseEvent {
    type: EventType.IntroduceYourselfSkipEvent;
}

export interface SharedEntityEvent extends BaseEvent {
    type: EventType.SharedEntityEvent;
    entityType?: EntityType;
    entityUrn: string;
    connectionUrn?: string;
    connectionUrns?: string[];
}

export interface UnsharedEntityEvent extends BaseEvent {
    type: EventType.UnsharedEntityEvent;
    entityType?: EntityType;
    entityUrn: string;
    connectionUrns: string[];
}

/**
 * Viewed the Home Page on the UI.
 */
export interface HomePageViewEvent extends BaseEvent {
    type: EventType.HomePageViewEvent;
}

/**
 * Viewed the Proposal Page on the UI.
 */
export interface InboxPageViewEvent extends BaseEvent {
    type: EventType.InboxPageViewEvent;
}

/**
 * Logged on successful new user sign up.
 */
export interface SignUpEvent extends BaseEvent {
    type: EventType.SignUpEvent;
    title: string;
}

/**
 * Logged on user successful login.
 */
export interface LogInEvent extends BaseEvent {
    type: EventType.LogInEvent;
}

/**
 * Logged on user successful logout.
 */
export interface LogOutEvent extends BaseEvent {
    type: EventType.LogOutEvent;
}

/**
 * Logged on user resetting their credentials
 */
export interface ResetCredentialsEvent extends BaseEvent {
    type: EventType.ResetCredentialsEvent;
}

/**
 * Logged on user successful search query.
 */
export interface SearchEvent extends BaseEvent {
    type: EventType.SearchEvent;
    query: string;
    entityTypeFilter?: EntityType;
    pageNumber: number;
    originPath: string;
    selectedQuickFilterValues?: string[];
    selectedQuickFilterTypes?: string[];
}

/**
 * Logged on user successful search query from the home page.
 */
export interface HomePageSearchEvent extends BaseEvent {
    type: EventType.HomePageSearchEvent;
    query: string;
    entityTypeFilter?: EntityType;
    pageNumber: number;
    selectedQuickFilterValues?: string[];
    selectedQuickFilterTypes?: string[];
}

/**
 * Logged on user search result click.
 */
export interface SearchResultsViewEvent extends BaseEvent {
    type: EventType.SearchResultsViewEvent;
    query: string;
    entityTypeFilter?: EntityType;
    page?: number;
    total: number;
    entityTypes: string[];
    filterFields: string[];
    filterCount: number;
    filterMode: FilterMode;
    searchVersion: string;
}

/**
 * Logged on user search result click.
 */
export interface SearchResultClickEvent extends BaseEvent {
    type: EventType.SearchResultClickEvent;
    query: string;
    entityUrn: string;
    entityType: EntityType;
    entityTypeFilter?: EntityType;
    index: number;
    total: number;
    pageNumber: number;
}

export interface SearchFiltersClearAllEvent extends BaseEvent {
    type: EventType.SearchFiltersClearAllEvent;
    total: number;
}

export interface SearchFiltersShowMoreEvent extends BaseEvent {
    type: EventType.SearchFiltersShowMoreEvent;
    activeFilterCount: number;
    hiddenFilterCount: number;
}

/**
 * Logged on user browse result click.
 */
export interface BrowseResultClickEvent extends BaseEvent {
    type: EventType.BrowseResultClickEvent;
    browsePath: string;
    entityType: EntityType;
    resultType: 'Entity' | 'Group';
    entityUrn?: string;
    groupName?: string;
}

/**
 * Logged on user browse result click from the home page.
 */
export interface HomePageBrowseResultClickEvent extends BaseEvent {
    type: EventType.HomePageBrowseResultClickEvent;
    entityType: EntityType;
}

/**
 * Logged when a user opens or closes the browse v2 sidebar
 */
export interface BrowseV2ToggleSidebarEvent extends BaseEvent {
    type: EventType.BrowseV2ToggleSidebarEvent;
    /** describes whether the user is opening or closing the browse v2 sidebar */
    action: 'open' | 'close';
}

/**
 * Logged when a user opens or closes a sidebar node
 */
export interface BrowseV2ToggleNodeEvent extends BaseEvent {
    type: EventType.BrowseV2ToggleNodeEvent;
    /** which section in the browse sidebar is being opened or closed */
    targetNode: 'entity' | 'environment' | 'platform' | 'browse';
    /** describes whether the user is opening or closing the section inside of the browse sidebar */
    action: 'open' | 'close';
    entity?: string;
    environment?: string;
    platform?: string;
    targetDepth: number;
}

/**
 * Logged when a user selects a browse node in the sidebar
 */
export interface BrowseV2SelectNodeEvent extends BaseEvent {
    type: EventType.BrowseV2SelectNodeEvent;
    targetNode: 'browse' | 'platform';
    action: 'select' | 'deselect';
    entity?: string;
    environment?: string;
    platform?: string;
    targetDepth: number;
}

/**
 * Logged when a user clicks a container link in the sidebar
 */
export interface BrowseV2EntityLinkClickEvent extends BaseEvent {
    type: EventType.BrowseV2EntityLinkClickEvent;
    targetNode: 'browse';
    entity?: string;
    environment?: string;
    platform?: string;
    targetDepth: number;
}

/**
 * Logged when user views an entity profile.
 */
export interface EntityViewEvent extends BaseEvent {
    type: EventType.EntityViewEvent;
    entityType: EntityType;
    entityUrn: string;
}

/**
 * Logged when user views a particular section of an entity profile.
 */
export interface EntitySectionViewEvent extends BaseEvent {
    type: EventType.EntitySectionViewEvent;
    entityType: EntityType;
    entityUrn: string;
    section: string;
}

/**
 * Logged when a user takes some action on an entity
 */
export const EntityActionType = {
    UpdateTags: 'UpdateTags',
    UpdateTerms: 'UpdateTerms',
    UpdateLinks: 'UpdateLinks',
    AddLink: 'AddLink',
    DeleteLink: 'DeleteLink',
    UpdateOwnership: 'UpdateOwnership',
    UpdateDocumentation: 'UpdateDocumentation',
    UpdateDescription: 'UpdateDescription',
    UpdateProperties: 'UpdateProperties',
    SetDomain: 'SetDomain',
    SetDataProduct: 'SetDataProduct',
    UpdateSchemaDescription: 'UpdateSchemaDescription',
    UpdateSchemaTags: 'UpdateSchemaTags',
    UpdateSchemaTerms: 'UpdateSchemaTerms',
    ClickExternalUrl: 'ClickExternalUrl',
    AddIncident: 'AddIncident',
    ResolvedIncident: 'ResolvedIncident',
    // figure out type of proposal
    ProposalCreated: 'ProposalCreated',
    ProposalAccepted: 'ProposalAccepted',
    ProposalRejected: 'ProposalRejected',
    ProposalsAccepted: 'ProposalsAccepted',
    ProposalsRejected: 'ProposalsRejected',
};

export enum ExternalLinkType {
    Custom = 'CUSTOM',
    Default = 'DEFAULT_EXTERNAL_URL',
}

export interface EntityActionEvent extends BaseEvent {
    type: EventType.EntityActionEvent;
    actionType: string;
    actionQualifier?: string;
    entityType?: EntityType;
    entityUrn: string;
    externalLinkType?: ExternalLinkType;
}

export interface BatchEntityActionEvent extends BaseEvent {
    type: EventType.BatchEntityActionEvent;
    actionType: string;
    entityUrns: string[];
}

export interface BatchProposalActionEvent extends BaseEvent {
    type: EventType.BatchProposalActionEvent;
    actionType: string;
    entityUrns: string[];
    proposalsCount: number;
    countByType: Partial<Record<ActionRequestType, number>>;
}

export interface RecommendationImpressionEvent extends BaseEvent {
    type: EventType.RecommendationImpressionEvent;
    moduleId: string;
    renderType: RecommendationRenderType;
    scenarioType: ScenarioType;
    // TODO: Determine whether we need to collect context parameters.
}

export interface RecommendationClickEvent extends BaseEvent {
    type: EventType.RecommendationClickEvent;
    renderId: string; // TODO : Determine whether we need a render id to join with click event.
    moduleId: string;
    renderType: RecommendationRenderType;
    scenarioType: ScenarioType;
    index?: number;
}

export interface HomePageRecommendationClickEvent extends BaseEvent {
    type: EventType.HomePageRecommendationClickEvent;
    renderId: string; // TODO : Determine whether we need a render id to join with click event.
    moduleId: string;
    renderType: RecommendationRenderType;
    scenarioType: ScenarioType;
    index?: number;
}

export interface VisualLineageViewEvent extends BaseEvent {
    type: EventType.VisualLineageViewEvent;
    entityType: EntityType;
    numUpstreams: number;
    numDownstreams: number;
    hasColumnLevelLineage: boolean;
    hasExpandableUpstreamsV2?: boolean;
    hasExpandableDownstreamsV2?: boolean;
    hasExpandableUpstreamsV3?: boolean;
    hasExpandableDownstreamsV3?: boolean;
}

export interface VisualLineageExpandGraphEvent extends BaseEvent {
    type: EventType.VisualLineageExpandGraphEvent;
    targetEntityType?: EntityType;
}

export interface SearchAcrossLineageEvent extends BaseEvent {
    type: EventType.SearchAcrossLineageEvent;
    query: string;
    entityTypeFilter?: EntityType;
    pageNumber: number;
    originPath: string;
    maxDegree?: string;
}
export interface SearchAcrossLineageResultsViewEvent extends BaseEvent {
    type: EventType.SearchAcrossLineageResultsViewEvent;
    query: string;
    entityTypeFilter?: EntityType;
    page?: number;
    total: number;
    maxDegree?: string;
    hasUserAppliedColumnFilter?: boolean;
    /** Whether search is scoped to a specific schema field URN (from navigation) */
    isSchemaFieldContext?: boolean;
}

export interface DownloadAsCsvEvent extends BaseEvent {
    type: EventType.DownloadAsCsvEvent;
    query: string;
    // optional parameter if its coming from inside an entity page
    entityUrn?: string;
    path: string;
}

export interface CreateAccessTokenEvent extends BaseEvent {
    type: EventType.CreateAccessTokenEvent;
    accessTokenType: string;
    duration: string;
}

export interface RevokeAccessTokenEvent extends BaseEvent {
    type: EventType.RevokeAccessTokenEvent;
}

export interface CreateGroupEvent extends BaseEvent {
    type: EventType.CreateGroupEvent;
}
export interface CreateInviteLinkEvent extends BaseEvent {
    type: EventType.CreateInviteLinkEvent;
    roleUrn?: string;
}

export interface RefreshInviteLinkEvent extends BaseEvent {
    type: EventType.RefreshInviteLinkEvent;
    roleUrn?: string;
}

export interface CreateResetCredentialsLinkEvent extends BaseEvent {
    type: EventType.CreateResetCredentialsLinkEvent;
    userUrn: string;
}

export interface DeleteEntityEvent extends BaseEvent {
    type: EventType.DeleteEntityEvent;
    entityUrn: string;
    entityType: EntityType;
}

export interface SelectUserRoleEvent extends BaseEvent {
    type: EventType.SelectUserRoleEvent;
    roleUrn: string;
    userUrn: string;
}

export interface SelectGroupRoleEvent extends BaseEvent {
    type: EventType.SelectGroupRoleEvent;
    roleUrn: string;
    groupUrn?: string;
}

export interface BatchSelectUserRoleEvent extends BaseEvent {
    type: EventType.BatchSelectUserRoleEvent;
    roleUrn: string;
    userUrns: string[];
}

// Policy events

export interface CreatePolicyEvent extends BaseEvent {
    type: EventType.CreatePolicyEvent;
}

export interface UpdatePolicyEvent extends BaseEvent {
    type: EventType.UpdatePolicyEvent;
    policyUrn: string;
}

export interface DeactivatePolicyEvent extends BaseEvent {
    type: EventType.DeactivatePolicyEvent;
    policyUrn: string;
}

export interface ActivatePolicyEvent extends BaseEvent {
    type: EventType.ActivatePolicyEvent;
    policyUrn: string;
}

export interface ShowSimplifiedHomepageEvent extends BaseEvent {
    type: EventType.ShowSimplifiedHomepageEvent;
}

export interface ShowStandardHomepageEvent extends BaseEvent {
    type: EventType.ShowStandardHomepageEvent;
}

export interface ShowV2ThemeEvent extends BaseEvent {
    type: EventType.ShowV2ThemeEvent;
}

export interface RevertV2ThemeEvent extends BaseEvent {
    type: EventType.RevertV2ThemeEvent;
}

export interface HomePageExploreAllClickEvent extends BaseEvent {
    type: EventType.HomePageExploreAllClickEvent;
}

export interface SearchBarExploreAllClickEvent extends BaseEvent {
    type: EventType.SearchBarExploreAllClickEvent;
}

export interface SearchResultsExploreAllClickEvent extends BaseEvent {
    type: EventType.SearchResultsExploreAllClickEvent;
}

// Business glossary events

export interface CreateGlossaryEntityEvent extends BaseEvent {
    type: EventType.CreateGlossaryEntityEvent;
    entityType: EntityType;
    parentNodeUrn?: string;
}

export interface CreateDomainEvent extends BaseEvent {
    type: EventType.CreateDomainEvent;
    parentDomainUrn?: string;
}

export interface MoveDomainEvent extends BaseEvent {
    type: EventType.MoveDomainEvent;
    oldParentDomainUrn?: string;
    parentDomainUrn?: string;
}

// Managed Ingestion Events

export interface IngestionTestConnectionEvent extends BaseEvent {
    type: EventType.IngestionTestConnectionEvent;
    sourceType: string;
    sourceUrn?: string;
    outcome?: string;
}

export interface IngestionViewAllClickEvent extends BaseEvent {
    type: EventType.IngestionViewAllClickEvent;
    executionUrn?: string;
}

export interface IngestionViewAllClickWarningEvent extends BaseEvent {
    type: EventType.IngestionViewAllClickWarningEvent;
    executionUrn?: string;
}

export interface IngestionExecutionResultViewedEvent extends BaseEvent {
    type: EventType.IngestionExecutionResultViewedEvent;
    executionUrn: string;
    executionStatus: string;
    viewedSection: string;
}

export interface IngestionSourceConfigurationImpressionEvent extends BaseEvent {
    type: EventType.IngestionSourceConfigurationImpressionEvent;
    viewedSection: 'SELECT_TEMPLATE' | 'DEFINE_RECIPE' | 'CREATE_SCHEDULE' | 'NAME_SOURCE';
    sourceType?: string;
    sourceUrn?: string;
}

export interface CreateIngestionSourceEvent extends BaseEvent {
    type: EventType.CreateIngestionSourceEvent;
    sourceType: string;
    sourceUrn?: string;
    interval?: string;
    numOwners?: number;
    outcome?: string;
}

export interface UpdateIngestionSourceEvent extends BaseEvent {
    type: EventType.UpdateIngestionSourceEvent;
    sourceType: string;
    sourceUrn: string;
    interval?: string;
    numOwners?: number;
    outcome?: string;
}

export interface DeleteIngestionSourceEvent extends BaseEvent {
    type: EventType.DeleteIngestionSourceEvent;
}

export interface ExecuteIngestionSourceEvent extends BaseEvent {
    type: EventType.ExecuteIngestionSourceEvent;
    sourceType?: string;
    sourceUrn?: string;
}

// TODO: Find a way to use this event
export interface SsoEvent extends BaseEvent {
    type: EventType.SsoEvent;
}

// SaaS only events
export interface CreateTestEvent extends BaseEvent {
    type: EventType.CreateTestEvent;
}

export interface UpdateTestEvent extends BaseEvent {
    type: EventType.UpdateTestEvent;
}

export interface DeleteTestEvent extends BaseEvent {
    type: EventType.DeleteTestEvent;
}

export interface CreateActionEvent extends BaseEvent {
    type: EventType.CreateActionEvent;
    actionType: string;
}

export interface UpdateActionEvent extends BaseEvent {
    type: EventType.UpdateActionEvent;
    actionType: string;
}

export interface DeleteActionEvent extends BaseEvent {
    type: EventType.DeleteActionEvent;
    actionType?: string;
}

export interface CreateAssertionMonitorEvent extends BaseEvent {
    type: EventType.CreateAssertionMonitorEvent;
    assertionType: string;
    entityUrn: string;
}

export interface UpdateAssertionMonitorEvent extends BaseEvent {
    type: EventType.UpdateAssertionMonitorEvent;
    assertionType: string;
    entityUrn: string;
}
export interface UpdateAssertionMetadataEvent extends BaseEvent {
    type: EventType.UpdateAssertionMetadataEvent;
    assertionType: string;
    assertionUrn: string;
    entityUrn: string;
}

export interface StartAssertionMonitorEvent extends BaseEvent {
    type: EventType.StartAssertionMonitorEvent;
    assertionType: string;
    assertionUrn: string;
    monitorUrn: string;
}

export interface StopAssertionMonitorEvent extends BaseEvent {
    type: EventType.StopAssertionMonitorEvent;
    assertionType: string;
    assertionUrn: string;
    monitorUrn: string;
}

export interface ViewAssertionNoteTabEvent extends BaseEvent {
    type: EventType.ViewAssertionNoteTabEvent;
    assertionUrn: string;
    entityUrn: string;
    assertionType: string;
}

export interface UpdateAssertionNoteEvent extends BaseEvent {
    type: EventType.UpdateAssertionNoteEvent;
    assertionUrn: string;
    entityUrn: string;
    assertionType: string;
}

export interface ManuallyCreateLineageEvent extends BaseEvent {
    type: EventType.ManuallyCreateLineageEvent;
    direction: Direction;
    sourceEntityType?: EntityType;
    sourceEntityPlatform?: string;
    destinationEntityType?: EntityType;
    destinationEntityPlatform?: string;
}

export interface ManuallyDeleteLineageEvent extends BaseEvent {
    type: EventType.ManuallyDeleteLineageEvent;
    direction: Direction;
    sourceEntityType?: EntityType;
    sourceEntityPlatform?: string;
    destinationEntityType?: EntityType;
    destinationEntityPlatform?: string;
}

/**
 * Emitted when a new View is created.
 */
export interface CreateViewEvent extends BaseEvent {
    type: EventType.CreateViewEvent;
    viewType?: DataHubViewType;
    filterFields: string[];
    entityTypes: string[];
    searchVersion: string;
}

/**
 * Emitted when an existing View is updated.
 */
export interface UpdateViewEvent extends BaseEvent {
    type: EventType.UpdateViewEvent;
    viewType?: DataHubViewType;
    urn: string;
    filterFields: string[];
    entityTypes: string[];
    searchVersion: string;
}

/**
 * Emitted when a user sets or clears their personal default view.
 */
export interface SetUserDefaultViewEvent extends BaseEvent {
    type: EventType.SetUserDefaultViewEvent;
    urn: string | null;
    viewType: DataHubViewType | null;
}

/**
 * Emitted when a user sets or clears the global default view.
 */
export interface SetGlobalDefaultViewEvent extends BaseEvent {
    type: EventType.SetGlobalDefaultViewEvent;
    urn: string | null;
}

export interface LineageGraphTimeRangeSelectionEvent extends BaseEvent {
    type: EventType.LineageGraphTimeRangeSelectionEvent;
    relativeStartDate: string;
    relativeEndDate: string;
}

export interface LineageTabTimeRangeSelectionEvent extends BaseEvent {
    type: EventType.LineageTabTimeRangeSelectionEvent;
    relativeStartDate: string;
    relativeEndDate: string;
}

export interface CreateQueryEvent extends BaseEvent {
    type: EventType.CreateQueryEvent;
}

export interface UpdateQueryEvent extends BaseEvent {
    type: EventType.UpdateQueryEvent;
}

export interface DeleteQueryEvent extends BaseEvent {
    type: EventType.DeleteQueryEvent;
}

export interface SelectAutoCompleteOption extends BaseEvent {
    type: EventType.SelectAutoCompleteOption;
    optionType: string;
    entityType?: EntityType;
    entityUrn?: string;
    showSearchBarAutocompleteRedesign?: boolean;
    apiVariant?: SearchBarApi;
}

export interface SelectQuickFilterEvent extends BaseEvent {
    type: EventType.SelectQuickFilterEvent;
    quickFilterType: string;
    quickFilterValue: string;
}

export interface DeselectQuickFilterEvent extends BaseEvent {
    type: EventType.DeselectQuickFilterEvent;
    quickFilterType: string;
    quickFilterValue: string;
}

export interface EmbedProfileViewEvent extends BaseEvent {
    type: EventType.EmbedProfileViewEvent;
    entityType: string;
    entityUrn: string;
}

export interface EmbedProfileViewInDataHubEvent extends BaseEvent {
    type: EventType.EmbedProfileViewInDataHubEvent;
    entityType: string;
    entityUrn: string;
}

export interface EmbedLookupNotFoundEvent extends BaseEvent {
    type: EventType.EmbedLookupNotFoundEvent;
    url: string;
    reason: EmbedLookupNotFoundReason;
}

export interface CreateBusinessAttributeEvent extends BaseEvent {
    type: EventType.CreateBusinessAttributeEvent;
    name: string;
}

export enum DocRequestCTASource {
    TaskCenter = 'TaskCenter',
    AssetPage = 'AssetPage',
}

export interface ClickDocRequestCTA extends BaseEvent {
    type: EventType.ClickDocRequestCTA;
    source: DocRequestCTASource;
}

export enum DocRequestView {
    ByQuestion = 'ByQuestion',
    ByAsset = 'ByAsset',
    BulkVerify = 'BulkVerify',
}

export interface CompleteDocRequestPrompt extends BaseEvent {
    type: EventType.CompleteDocRequestPrompt;
    source: DocRequestView;
    promptId: string;
    required: boolean;
    numAssets: number;
}

export interface CompleteVerification extends BaseEvent {
    type: EventType.CompleteVerification;
    source: DocRequestView;
    numAssets: number;
}

export interface OpenTaskCenter extends BaseEvent {
    type: EventType.OpenTaskCenter;
}

export interface SlackIntegrationSuccessEvent extends BaseEvent {
    type: EventType.SlackIntegrationSuccessEvent;
    configType: string;
}

export interface SlackIntegrationErrorEvent extends BaseEvent {
    type: EventType.SlackIntegrationErrorEvent;
    configType: string;
}

export interface SubscriptionCreateSuccessEvent extends BaseEvent {
    type: EventType.SubscriptionCreateSuccessEvent;
    subscriptionUrn: string;
    entityUrn: string;
    entityType: EntityType;
    entityChangeTypes: Array<EntityChangeType>;
    sinkTypes: Array<NotificationSinkType>;
    actorType: ActorType;
}

export interface SubscriptionCreateErrorEvent extends BaseEvent {
    type: EventType.SubscriptionCreateErrorEvent;
    entityUrn: string;
    entityType: EntityType;
    entityChangeTypes: Array<EntityChangeType>;
    sinkTypes: Array<NotificationSinkType>;
    actorType: ActorType;
}

export interface SubscriptionUpdateSuccessEvent extends BaseEvent {
    type: EventType.SubscriptionUpdateSuccessEvent;
    subscriptionUrn: string;
    entityUrn: string;
    entityType: EntityType;
    entityChangeTypes: Array<EntityChangeType>;
    entityChangeTypesAdded: Array<EntityChangeType>;
    entityChangeTypesRemoved: Array<EntityChangeType>;
    sinkTypes: Array<NotificationSinkType>;
    sinkTypesAdded: Array<NotificationSinkType>;
    sinkTypesRemoved: Array<NotificationSinkType>;
    actorType: ActorType;
}

export interface SubscriptionUpdateErrorEvent extends BaseEvent {
    type: EventType.SubscriptionUpdateErrorEvent;
    subscriptionUrn: string;
    entityUrn: string;
    entityType: EntityType;
    entityChangeTypes: Array<EntityChangeType>;
    sinkTypes: Array<NotificationSinkType>;
    actorType: ActorType;
}

export interface SubscriptionDeleteSuccessEvent extends BaseEvent {
    type: EventType.SubscriptionDeleteSuccessEvent;
    subscriptionUrn: string;
    entityUrn: string;
    entityType: EntityType;
    entityChangeTypes: Array<EntityChangeType>;
    sinkTypes: Array<NotificationSinkType>;
    actorType: ActorType;
}

export interface SubscriptionDeleteErrorEvent extends BaseEvent {
    type: EventType.SubscriptionDeleteErrorEvent;
    subscriptionUrn: string;
    entityUrn: string;
    entityType: EntityType;
    entityChangeTypes: Array<EntityChangeType>;
    sinkTypes: Array<NotificationSinkType>;
    actorType: ActorType;
}

export interface NotificationSettingsSuccessEvent extends BaseEvent {
    type: EventType.NotificationSettingsSuccessEvent;
    sinkTypes: Array<NotificationSinkType>;
    sinkTypesAdded: Array<NotificationSinkType>;
    sinkTypesRemoved: Array<NotificationSinkType>;
    actorType: ActorType;
}

export interface NotificationSettingsErrorEvent extends BaseEvent {
    type: EventType.NotificationSettingsErrorEvent;
    sinkTypes: Array<NotificationSinkType>;
    sinkTypesAdded: Array<NotificationSinkType>;
    sinkTypesRemoved: Array<NotificationSinkType>;
    actorType: ActorType;
}

export interface CreateActionWorkflowFormRequestEvent extends BaseEvent {
    type: EventType.CreateActionWorkflowFormRequest;
    workflowUrn: string;
    workflowName: string;
    workflowCategory?: ActionWorkflowCategory | null;
    actionRequestUrn: string;
}

export interface ReviewActionWorkflowFormRequestEvent extends BaseEvent {
    type: EventType.ReviewActionWorkflowFormRequest;
    actionRequestUrn: string;
    actionType: ActionRequestResult;
    workflowUrn: string;
}

export interface BatchReviewActionWorkflowFormRequestEvent extends BaseEvent {
    type: EventType.BatchReviewActionWorkflowFormRequest;
    actionRequestUrns: string[];
    actionType: ActionRequestResult;
}

export interface ExpandLineageEvent extends BaseEvent {
    type: EventType.ExpandLineageEvent;
    direction: LineageDirection;
    levelsExpanded: '1' | 'all';
    entityUrn: string;
    entityType: EntityType;
}

export interface ContractLineageEvent extends BaseEvent {
    type: EventType.ContractLineageEvent;
    direction: LineageDirection;
    entityUrn: string;
    entityType?: EntityType;
}

export interface ShowHideLineageColumnsEvent extends BaseEvent {
    type: EventType.ShowHideLineageColumnsEvent;
    action: 'show' | 'hide';
    entityUrn: string;
    entityType: EntityType;
    entityPlatformUrn?: string;
}

export interface SearchLineageColumnsEvent extends BaseEvent {
    type: EventType.SearchLineageColumnsEvent;
    entityUrn: string;
    entityType: EntityType;
    searchTextLength: number;
}

export interface FilterLineageColumnsEvent extends BaseEvent {
    type: EventType.FilterLineageColumnsEvent;
    action: 'enable' | 'disable';
    entityUrn: string;
    entityType: EntityType;
    shownCount: number;
}

export interface DrillDownLineageEvent extends BaseEvent {
    type: EventType.DrillDownLineageEvent;
    action: 'select' | 'deselect';
    entityUrn: string;
    entityType: EntityType;
    parentUrn: string;
    parentEntityType: EntityType;
    dataType?: string;
}

export interface CreateFormClickEvent extends BaseEvent {
    type: EventType.CreateFormClickEvent;
}

interface FormEvent extends BaseEvent {
    formUrn: string;
    formType: FormType;
    noOfQuestions: number;
    areOwnersAssigned: boolean;
    noOfAssetsAssigned?: number;
    notificationsEnabled?: boolean;
}
export interface SaveFormAsDraftEvent extends FormEvent {
    type: EventType.SaveFormAsDraftEvent;
}

export interface PublishFormEvent extends FormEvent {
    type: EventType.PublishFormEvent;
}

export interface UnpublishFormEvent extends FormEvent {
    type: EventType.UnpublishFormEvent;
}

export interface DeleteFormEvent extends FormEvent {
    type: EventType.DeleteFormEvent;
}

interface QuestionEvent extends BaseEvent {
    formUrn?: string;
    questionId: string;
    questionType: FormPromptType;
    required: boolean;
    allowMultiple?: boolean;
    restrictedGlossaryTerms?: boolean;
    restrictedOwners?: boolean;
    restrictedOwnershipTypes?: boolean;
    restrictedDomains?: boolean;
}

export interface CreateQuestionEvent extends QuestionEvent {
    type: EventType.CreateQuestionEvent;
}

export interface EditQuestionEvent extends QuestionEvent {
    type: EventType.EditQuestionEvent;
}

export interface CreateStructuredPropertyClickEvent extends BaseEvent {
    type: EventType.CreateStructuredPropertyClickEvent;
}

interface StructuredPropertyEvent extends BaseEvent {
    propertyType: string;
    appliesTo: string[];
    qualifiedName?: string;
    allowedAssetTypes?: string[];
    allowedValues?: AllowedValue[];
    cardinality?: PropertyCardinality;
    showInFilters?: StructuredPropertyFilterStatus;
    isHidden: boolean;
    showInSearchFilters: boolean;
    showAsAssetBadge: boolean;
    showInAssetSummary: boolean;
    hideInAssetSummaryWhenEmpty: boolean;
    showInColumnsTable: boolean;
}

export interface CreateStructuredPropertyEvent extends StructuredPropertyEvent {
    type: EventType.CreateStructuredPropertyEvent;
}

export interface EditStructuredPropertyEvent extends StructuredPropertyEvent {
    type: EventType.EditStructuredPropertyEvent;
    propertyUrn: string;
}

export interface DeleteStructuredPropertyEvent extends StructuredPropertyEvent {
    type: EventType.DeleteStructuredPropertyEvent;
    propertyUrn: string;
}

export interface ViewStructuredPropertyEvent extends BaseEvent {
    type: EventType.ViewStructuredPropertyEvent;
    propertyUrn: string;
}

interface StructuredPropertyOnAssetEvent extends BaseEvent {
    propertyUrn: string;
    propertyType: string;
    assetUrn: string;
    assetType: EntityType;
}
export interface ApplyStructuredPropertyEvent extends StructuredPropertyOnAssetEvent {
    type: EventType.ApplyStructuredPropertyEvent;
    values: PropertyValueInput[];
}

export interface UpdateStructuredPropertyOnAssetEvent extends StructuredPropertyOnAssetEvent {
    type: EventType.UpdateStructuredPropertyOnAssetEvent;
    values: PropertyValueInput[];
}

export interface ProposeStructuredPropertyEvent extends StructuredPropertyOnAssetEvent {
    type: EventType.ProposeStructuredPropertiesMutation;
    values: PropertyValueInput[];
}

export interface ProposeDomainEvent extends BaseEvent {
    type: EventType.ProposeDomainMutation;
    // The target entity urn for the proposal
    resourceUrn: string;
    // The domain urn which is being proposed
    domainUrn: string;
    description?: string;
}

export interface ProposeOwnersEvent extends BaseEvent {
    type: EventType.ProposeOwnersMutation;
    // The target entity urn for the proposal
    resourceUrn: string;
    owners: OwnerInput[];
    description?: string;
}

export interface RemoveStructuredPropertyEvent extends StructuredPropertyOnAssetEvent {
    type: EventType.RemoveStructuredPropertyEvent;
}

export interface InferDocsClickEvent extends BaseEvent {
    type: EventType.InferDocsClickEvent;
    surface:
        | 'schema-table'
        | 'schema-profile'
        | 'schema-docs-editor'
        | 'entity-sidebar'
        | 'entity-docs-tab'
        | 'entity-docs-editor'
        | 'query-viewer-modal'
        | 'query-builder-form';
}

export interface AcceptInferredDocsEvent extends BaseEvent {
    type: EventType.AcceptInferredDocs;
}

export interface DeclineInferredDocsEvent extends BaseEvent {
    type: EventType.DeclineInferredDocs;
}

export type ObfuscatedOidcSettings = { [k in keyof Partial<OidcSettings>]: boolean | string };
export interface SSOConfigurationEvent extends BaseEvent {
    type: EventType.SSOConfigurationEvent;
    action: 'enable-sso' | 'disable-sso' | 'initialize-sso' | 'update-sso' | 'expand-advanced';
    oldSettings?: ObfuscatedOidcSettings;
    newSettings?: ObfuscatedOidcSettings;
    isAdvancedVisible?: boolean; // true if advanced section is opened when user is hitting save
}

export interface LinkAssetVersionEvent extends BaseEvent {
    type: EventType.LinkAssetVersionEvent;
    newAssetUrn: string;
    oldAssetUrn?: string;
    versionSetUrn?: string;
    entityType: EntityType;
}

export interface UnlinkAssetVersionEvent extends BaseEvent {
    type: EventType.UnlinkAssetVersionEvent;
    assetUrn: string;
    versionSetUrn?: string;
    entityType: EntityType;
}

export interface ShowAllVersionsEvent extends BaseEvent {
    type: EventType.ShowAllVersionsEvent;
    assetUrn: string;
    versionSetUrn?: string;
    entityType: EntityType;
    numVersions?: number;
    uiLocation: 'preview' | 'more-options';
}

export interface SearchBarFilterEvent extends BaseEvent {
    type: EventType.SearchBarFilter;
    field: string; // the filter field
    values: string[]; // the values being filtered for
}

export interface FormByEntityNavigateEvent extends BaseEvent {
    type: EventType.FormByEntityNavigate;
    direction: 'forward' | 'backward';
}

export interface FormViewToggleEvent extends BaseEvent {
    type: EventType.FormViewToggle;
    formView: 'byAsset' | 'byQuestion';
}

export interface FormAnalyticsTabSelectEvent extends BaseEvent {
    type: EventType.FormAnalyticsTabSelect;
    tabName: string;
}

export interface FormAnalyticsDownloadCsvEvent extends BaseEvent {
    type: EventType.FormAnalyticsDownloadCsv;
    selectedTab: string;
}

export interface FormAnalyticsTabFilterEvent extends BaseEvent {
    type: EventType.FormAnalyticsTabFilter;
    selectedTab: string;
}

export interface ClickCreateAssertionEvent extends BaseEvent {
    type: EventType.ClickCreateAssertion;
    platform?: string | null;
    chartName?: string | null;
}

export interface ClickBulkCreateAssertionEvent extends BaseEvent {
    type: EventType.ClickBulkCreateAssertion;
    surface: 'field-metric-assertion-builder' | 'dataset-health';
    entityUrn?: string | null;
}

export interface BulkCreateAssertionSubmissionFailedEvent extends BaseEvent {
    type: EventType.BulkCreateAssertionSubmissionFailedEvent;
    surface: 'field-metric-assertion-builder' | 'dataset-health';
    error: string;
}

export interface BulkCreateAssertionSubmissionEvent extends BaseEvent {
    type: EventType.BulkCreateAssertionSubmissionEvent;
    surface: 'field-metric-assertion-builder' | 'dataset-health';
    entityCount: number;
    hasFreshnessAssertion: boolean;
    hasFieldMetricAssertion: boolean;
    hasVolumeAssertion: boolean;
    hasSubscription: boolean;
}

export interface BulkCreateAssertionCompletedEvent extends BaseEvent {
    type: EventType.BulkCreateAssertionCompletedEvent;
    surface: 'field-metric-assertion-builder' | 'dataset-health';
    entityCount: number;
    failedAssertionCount: number;
    successAssertionCount: number;
    totalAssertionCount: number;
    hasFreshnessAssertion: boolean;
    hasFieldMetricAssertion: boolean;
    hasVolumeAssertion: boolean;
    hasSubscription: boolean;
    successSubscriptionCount: number;
    failedSubscriptionCount: number;
}

export interface ClickUserProfileEvent extends BaseEvent {
    type: EventType.ClickUserProfile;
    location?: 'statsTabTopUsers'; // add more locations here
}

export interface ClickViewDocumentationEvent extends BaseEvent {
    type: EventType.ClickViewDocumentation;
    link: string;
    location: 'statsTab'; // add more locations here
}

export enum HomePageModule {
    YouRecentlyViewed = 'YouRecentlyViewed',
    Discover = 'Discover',
    Announcements = 'Announcements',
    PersonalSidebar = 'PersonalSidebar',
    SidebarAnnouncements = 'SidebarAnnouncements',
}

export interface HomePageClickEvent extends BaseEvent {
    type: EventType.HomePageClick;
    module: HomePageModule;
    section?: string;
    subSection?: string;
    value?: string; // what was actually clicked ie. an entity urn to go to a page, or "View all" for a section
}

export interface GiveAnomalyFeedbackEvent extends BaseEvent {
    type: EventType.GiveAnomalyFeedback;
    feedbackType: 'missedAlarm' | 'falseAlarm';
    assertionType: AssertionType | 'Unknown';
    runEventTimeMillisFromNow?: number;
    datasetUrn?: string;
    inferenceDetails: {
        sensitivity?: number;
        hasExclusionWindows: boolean;
        lookbackDays?: number;
    };
}

export interface UndoAnomalyFeedbackEvent extends BaseEvent {
    type: EventType.UndoAnomalyFeedback;
    assertionType: AssertionType | 'Unknown';
    runEventTimeMillisFromNow?: number;
    datasetUrn?: string;
    inferenceDetails: {
        sensitivity?: number;
        hasExclusionWindows: boolean;
        lookbackDays?: number;
    };
}

export interface RetrainAsNewNormalEvent extends BaseEvent {
    type: EventType.RetrainAsNewNormal;
    assertionType: AssertionType | 'Unknown';
    runEventTimeMillisFromNow?: number;
    datasetUrn?: string;
    inferenceDetails: {
        sensitivity?: number;
        hasExclusionWindows: boolean;
        lookbackDays?: number;
    };
}

export interface DatasetHealthFilterEvent extends BaseEvent {
    type: EventType.DatasetHealthFilterEvent;
    tabType: 'AssertionsByAssertion' | 'AssertionsByAsset' | 'IncidentsByAsset' | 'IncidentsByIncident';
    filterType: 'search' | 'filter' | 'timeRange';
    filterSubType?: string;
    content:
        | {
              filterValues: string[];
          }
        | {
              filterValue: string;
          };
}

export interface DatasetHealthClickEvent extends BaseEvent {
    type: EventType.DatasetHealthClickEvent;
    tabType: 'AssertionsByAssertion' | 'AssertionsByAsset' | 'IncidentsByAsset' | 'IncidentsByIncident';
    target: 'asset_assertions' | 'asset_incidents' | 'assertion' | 'incident';
    subTarget?: string;
    targetUrn?: string;
}

export interface NavBarExpandCollapseEvent extends BaseEvent {
    type: EventType.NavBarExpandCollapse;
    isExpanding: boolean; // whether this action is expanding or collapsing the nav bar
}

export interface NavBarItemClickEvent extends BaseEvent {
    type: EventType.NavBarItemClick;
    label: string; // the label of the item that is clicks from the nav sidebar
}

export interface FilterStatsPageEvent extends BaseEvent {
    type: EventType.FilterStatsPage;
    platform: string | null;
}

export interface FilterStatsChartLookBackEvent extends BaseEvent {
    type: EventType.FilterStatsChartLookBack;
    lookBackValue: string;
    chartName: string;
}

export interface WelcomeToDataHubModalViewEvent extends BaseEvent {
    type: EventType.WelcomeToDataHubModalViewEvent;
}

export interface WelcomeToDataHubModalInteractEvent extends BaseEvent {
    type: EventType.WelcomeToDataHubModalInteractEvent;
    currentSlide: number;
    totalSlides: number;
}

export interface WelcomeToDataHubModalExitEvent extends BaseEvent {
    type: EventType.WelcomeToDataHubModalExitEvent;
    currentSlide: number;
    totalSlides: number;
    exitMethod: 'close_button' | 'get_started_button' | 'outside_click' | 'escape_key';
}

export interface WelcomeToDataHubModalClickViewDocumentationEvent extends BaseEvent {
    type: EventType.WelcomeToDataHubModalClickViewDocumentationEvent;
    url: string;
}

export interface ProductTourButtonClickEvent extends BaseEvent {
    type: EventType.ProductTourButtonClickEvent;
    originPage: string; // Page where the button was clicked
}

export interface ClickProductUpdateEvent extends BaseEvent {
    type: EventType.ClickProductUpdate;
    id: string;
    url: string;
}

export interface HomePageTemplateModuleCreateEvent extends BaseEvent {
    type: EventType.HomePageTemplateModuleCreate;
    templateUrn: string;
    isPersonal: boolean;
    moduleType: DataHubPageModuleType;
    location: PageTemplateSurfaceType;
}

export interface HomePageTemplateModuleAddEvent extends BaseEvent {
    type: EventType.HomePageTemplateModuleAdd;
    templateUrn: string;
    isPersonal: boolean;
    moduleType: DataHubPageModuleType;
    location: PageTemplateSurfaceType;
}

export interface HomePageTemplateModuleUpdateEvent extends BaseEvent {
    type: EventType.HomePageTemplateModuleUpdate;
    templateUrn: string;
    isPersonal: boolean;
    moduleType: DataHubPageModuleType;
    location: PageTemplateSurfaceType;
}

export interface HomePageTemplateModuleDeleteEvent extends BaseEvent {
    type: EventType.HomePageTemplateModuleDelete;
    templateUrn: string;
    isPersonal: boolean;
    moduleType: DataHubPageModuleType;
    location: PageTemplateSurfaceType;
}

export interface HomePageTemplateModuleMoveEvent extends BaseEvent {
    type: EventType.HomePageTemplateModuleMove;
    templateUrn: string;
    isPersonal: boolean;
    location: PageTemplateSurfaceType;
}

export interface HomePageTemplateModuleModalCreateOpenEvent extends BaseEvent {
    type: EventType.HomePageTemplateModuleModalCreateOpen;
    moduleType: DataHubPageModuleType;
    location: PageTemplateSurfaceType;
}

export interface HomePageTemplateModuleModalEditOpenEvent extends BaseEvent {
    type: EventType.HomePageTemplateModuleModalEditOpen;
    moduleType: DataHubPageModuleType;
    location: PageTemplateSurfaceType;
}

export interface HomePageTemplateModuleModalCancelEvent extends BaseEvent {
    type: EventType.HomePageTemplateModuleModalCancel;
    moduleType: DataHubPageModuleType;
    location: PageTemplateSurfaceType;
}

export interface HomePageTemplateGlobalTemplateEditingStartEvent extends BaseEvent {
    type: EventType.HomePageTemplateGlobalTemplateEditingStart;
}

export interface HomePageTemplateGlobalTemplateEditingDoneEvent extends BaseEvent {
    type: EventType.HomePageTemplateGlobalTemplateEditingDone;
}

export interface HomePageTemplateResetToGlobalTemplateEvent extends BaseEvent {
    type: EventType.HomePageTemplateResetToGlobalTemplate;
}

export interface HomePageTemplateModuleAssetClickEvent extends BaseEvent {
    type: EventType.HomePageTemplateModuleAssetClick;
    moduleType: DataHubPageModuleType;
    assetUrn: string;
    location: PageTemplateSurfaceType;
}

export interface HomePageTemplateModuleExpandClickEvent extends BaseEvent {
    type: EventType.HomePageTemplateModuleExpandClick;
    moduleType: DataHubPageModuleType;
    assetUrn: string;
    location: PageTemplateSurfaceType;
}

export interface HomePageTemplateModuleViewAllClickEvent extends BaseEvent {
    type: EventType.HomePageTemplateModuleViewAllClick;
    moduleType: DataHubPageModuleType;
    location: PageTemplateSurfaceType;
}

export interface HomePageTemplateModuleLinkClickEvent extends BaseEvent {
    type: EventType.HomePageTemplateModuleLinkClick;
    link: string;
}

export interface HomePageTemplateModuleAnnouncementDismissEvent extends BaseEvent {
    type: EventType.HomePageTemplateModuleAnnouncementDismiss;
}

export interface SetDeprecationEvent extends BaseEvent {
    type: EventType.SetDeprecation;
    entityUrns: string[];
    deprecated: boolean;
    resources?: ResourceRefInput[];
}

// Invite Users Events

export interface ClickInviteUsersCTAEvent extends BaseEvent {
    type: EventType.ClickInviteUsersCTAEvent;
    source: 'settings_page' | 'user_management' | 'nav_menu';
}

export interface ClickCopyInviteLinkEvent extends BaseEvent {
    type: EventType.ClickCopyInviteLinkEvent;
    roleUrn: string;
}

export interface ClickInviteViaEmailEvent extends BaseEvent {
    type: EventType.ClickInviteViaEmailEvent;
    roleUrn: string;
    emailList?: string[];
    emailCount?: number;
    enteredInvalidEmail: boolean;
}

export interface ClickInviteRecommendedUserEvent extends BaseEvent {
    type: EventType.ClickInviteRecommendedUserEvent;
    roleUrn: string;
    userEmail: string;
    location: 'invite_users_modal' | 'recommended_users_preview' | 'recommended_users_list';
    recommendationType: 'top_user';
    recommendationIndex?: number;
}

export interface ClickBulkInviteRecommendedUsersEvent extends BaseEvent {
    type: EventType.ClickBulkInviteRecommendedUsersEvent;
    roleUrn: string;
    userEmails: string; // comma-separated list of emails
    userCount: number;
    location: 'recommended_users_list';
    recommendationType: 'top_user';
}

export interface ClickBulkDismissRecommendedUsersEvent extends BaseEvent {
    type: EventType.ClickBulkDismissRecommendedUsersEvent;
    userEmails: string; // comma-separated list of emails
    userCount: number;
    location: 'recommended_users_list';
    recommendationType: 'top_user';
}

export interface InviteUserErrorEvent extends BaseEvent {
    type: EventType.InviteUserErrorEvent;
    roleUrn: string;
    emailList?: string[];
    inviteMethod: 'email' | 'recommended_user';
    errorMessage: string;
}

export interface AssetPageAddSummaryElementEvent extends BaseEvent {
    type: EventType.AssetPageAddSummaryElement;
    templateUrn: string;
    elementType: SummaryElementType;
}

export interface AssetPageRemoveSummaryElementEvent extends BaseEvent {
    type: EventType.AssetPageRemoveSummaryElement;
    templateUrn: string;
    elementType: SummaryElementType;
}

export interface AssetPageReplaceSummaryElementEvent extends BaseEvent {
    type: EventType.AssetPageReplaceSummaryElement;
    templateUrn: string;
    currentElementType: SummaryElementType;
    newElementType: SummaryElementType;
}

interface GoToLogicalParentEvent extends BaseEvent {
    type: EventType.GoToLogicalParentEvent;
    entityUrn: string;
    parentUrn?: string;
}

interface GoToPhysicalChildEvent extends BaseEvent {
    type: EventType.GoToPhysicalChildEvent;
    entityUrn: string;
    childUrn?: string;
}

interface GoToLogicalParentColumnEvent extends BaseEvent {
    type: EventType.GoToLogicalParentColumnEvent;
    entityUrn: string;
    parentUrn?: string;
}

interface GoToPhysicalChildColumnEvent extends BaseEvent {
    type: EventType.GoToPhysicalChildColumnEvent;
    entityUrn: string;
    childUrn?: string;
}

export interface FileUploadAttemptEvent extends BaseEvent {
    type: EventType.FileUploadAttemptEvent;
    fileType: string;
    fileSize: number;
    scenario: UploadDownloadScenario;
    source: 'drag-and-drop' | 'button';
    assetUrn?: string;
    schemaFieldUrn?: string;
}

export interface FileUploadFailedEvent extends BaseEvent {
    type: EventType.FileUploadFailedEvent;
    fileType: string;
    fileSize: number;
    scenario: UploadDownloadScenario;
    source: 'drag-and-drop' | 'button';
    assetUrn?: string;
    schemaFieldUrn?: string;
    failureType: FileUploadFailureType;
    comment?: string;
}

export interface FileUploadSucceededEvent extends BaseEvent {
    type: EventType.FileUploadSucceededEvent;
    fileType: string;
    fileSize: number;
    scenario: UploadDownloadScenario;
    source: 'drag-and-drop' | 'button';
    assetUrn?: string;
    schemaFieldUrn?: string;
}

export interface FileDownloadViewEvent extends BaseEvent {
    type: EventType.FileDownloadViewEvent;
    // These fields aren't accessible while downloading
    // fileType: string;
    // fileSize: number;
    scenario: UploadDownloadScenario;
    assetUrn?: string;
    schemaFieldUrn?: string;
}

/**
 * Event consisting of a union of specific event types.
 */
export type Event =
    | PageViewEvent
    | HomePageViewEvent
    | IntroduceYourselfViewEvent
    | IntroduceYourselfSubmitEvent
    | IntroduceYourselfSkipEvent
    | SignUpEvent
    | LogInEvent
    | LogOutEvent
    | ResetCredentialsEvent
    | SearchEvent
    | HomePageSearchEvent
    | HomePageExploreAllClickEvent
    | SearchBarExploreAllClickEvent
    | SearchResultsExploreAllClickEvent
    | SearchResultsViewEvent
    | SearchResultClickEvent
    | SearchFiltersClearAllEvent
    | SearchFiltersShowMoreEvent
    | BrowseResultClickEvent
    | HomePageBrowseResultClickEvent
    | BrowseV2ToggleSidebarEvent
    | BrowseV2ToggleNodeEvent
    | BrowseV2SelectNodeEvent
    | BrowseV2EntityLinkClickEvent
    | EntityViewEvent
    | EntitySectionViewEvent
    | EntityActionEvent
    | RecommendationImpressionEvent
    | SearchAcrossLineageEvent
    | SearchAcrossLineageResultsViewEvent
    | VisualLineageViewEvent
    | VisualLineageExpandGraphEvent
    | DownloadAsCsvEvent
    | RecommendationClickEvent
    | HomePageRecommendationClickEvent
    | BatchEntityActionEvent
    | BatchProposalActionEvent
    | CreateAccessTokenEvent
    | RevokeAccessTokenEvent
    | CreateGroupEvent
    | CreateInviteLinkEvent
    | RefreshInviteLinkEvent
    | CreateResetCredentialsLinkEvent
    | DeleteEntityEvent
    | SelectUserRoleEvent
    | SelectGroupRoleEvent
    | BatchSelectUserRoleEvent
    | CreatePolicyEvent
    | UpdatePolicyEvent
    | DeactivatePolicyEvent
    | ActivatePolicyEvent
    | ShowSimplifiedHomepageEvent
    | ShowStandardHomepageEvent
    | CreateGlossaryEntityEvent
    | CreateDomainEvent
    | MoveDomainEvent
    | CreateIngestionSourceEvent
    | UpdateIngestionSourceEvent
    | DeleteIngestionSourceEvent
    | ExecuteIngestionSourceEvent
    | ShowV2ThemeEvent
    | RevertV2ThemeEvent
    | SsoEvent
    | CreateTestEvent
    | UpdateTestEvent
    | DeleteTestEvent
    | CreateViewEvent
    | UpdateViewEvent
    | SetUserDefaultViewEvent
    | SetGlobalDefaultViewEvent
    | ManuallyCreateLineageEvent
    | ManuallyDeleteLineageEvent
    | LineageGraphTimeRangeSelectionEvent
    | LineageTabTimeRangeSelectionEvent
    | CreateQueryEvent
    | UpdateQueryEvent
    | DeleteQueryEvent
    | SelectAutoCompleteOption
    | SelectQuickFilterEvent
    | DeselectQuickFilterEvent
    | EmbedProfileViewEvent
    | EmbedProfileViewInDataHubEvent
    | EmbedLookupNotFoundEvent
    | CreateBusinessAttributeEvent
    | ClickDocRequestCTA
    | CompleteDocRequestPrompt
    | CompleteVerification
    | OpenTaskCenter
    | GoToLogicalParentEvent
    | GoToPhysicalChildEvent
    | GoToLogicalParentColumnEvent
    | GoToPhysicalChildColumnEvent
    | CreateAssertionMonitorEvent
    | UpdateAssertionMonitorEvent
    | UpdateAssertionMetadataEvent
    | StartAssertionMonitorEvent
    | StopAssertionMonitorEvent
    | ViewAssertionNoteTabEvent
    | UpdateAssertionNoteEvent
    | SlackIntegrationSuccessEvent
    | SlackIntegrationErrorEvent
    | SubscriptionCreateSuccessEvent
    | SubscriptionCreateErrorEvent
    | SubscriptionUpdateSuccessEvent
    | SubscriptionUpdateErrorEvent
    | SubscriptionDeleteSuccessEvent
    | SubscriptionDeleteErrorEvent
    | NotificationSettingsSuccessEvent
    | NotificationSettingsErrorEvent
    | InboxPageViewEvent
    | SharedEntityEvent
    | UnsharedEntityEvent
    | ExpandLineageEvent
    | ContractLineageEvent
    | ShowHideLineageColumnsEvent
    | SearchLineageColumnsEvent
    | FilterLineageColumnsEvent
    | DrillDownLineageEvent
    | InferDocsClickEvent
    | AcceptInferredDocsEvent
    | DeclineInferredDocsEvent
    | CreateFormClickEvent
    | SaveFormAsDraftEvent
    | PublishFormEvent
    | UnpublishFormEvent
    | DeleteFormEvent
    | CreateQuestionEvent
    | EditQuestionEvent
    | CreateStructuredPropertyClickEvent
    | CreateStructuredPropertyEvent
    | EditStructuredPropertyEvent
    | DeleteStructuredPropertyEvent
    | ViewStructuredPropertyEvent
    | ApplyStructuredPropertyEvent
    | UpdateStructuredPropertyOnAssetEvent
    | RemoveStructuredPropertyEvent
    | ProposeStructuredPropertyEvent
    | SSOConfigurationEvent
    | ProposeDomainEvent
    | ProposeOwnersEvent
    | LinkAssetVersionEvent
    | UnlinkAssetVersionEvent
    | ShowAllVersionsEvent
    | NavBarExpandCollapseEvent
    | NavBarItemClickEvent
    | HomePageClickEvent
    | SearchBarFilterEvent
    | HomePageTemplateModuleCreateEvent
    | HomePageTemplateModuleAddEvent
    | HomePageTemplateModuleUpdateEvent
    | HomePageTemplateModuleDeleteEvent
    | HomePageTemplateModuleMoveEvent
    | HomePageTemplateModuleModalCreateOpenEvent
    | HomePageTemplateModuleModalEditOpenEvent
    | HomePageTemplateModuleModalCancelEvent
    | HomePageTemplateGlobalTemplateEditingStartEvent
    | HomePageTemplateGlobalTemplateEditingDoneEvent
    | HomePageTemplateResetToGlobalTemplateEvent
    | HomePageTemplateModuleAssetClickEvent
    | HomePageTemplateModuleExpandClickEvent
    | HomePageTemplateModuleViewAllClickEvent
    | HomePageTemplateModuleLinkClickEvent
    | HomePageTemplateModuleAnnouncementDismissEvent
    | FormByEntityNavigateEvent
    | FormViewToggleEvent
    | FormAnalyticsTabSelectEvent
    | FormAnalyticsDownloadCsvEvent
    | FormAnalyticsTabFilterEvent
    | FilterStatsPageEvent
    | FilterStatsChartLookBackEvent
    | ClickCreateAssertionEvent
    | ClickBulkCreateAssertionEvent
    | BulkCreateAssertionSubmissionEvent
    | BulkCreateAssertionSubmissionFailedEvent
    | BulkCreateAssertionCompletedEvent
    | ClickUserProfileEvent
    | ClickViewDocumentationEvent
    | GiveAnomalyFeedbackEvent
    | UndoAnomalyFeedbackEvent
    | RetrainAsNewNormalEvent
    | CreateActionWorkflowFormRequestEvent
    | BatchReviewActionWorkflowFormRequestEvent
    | ReviewActionWorkflowFormRequestEvent
    | ClickProductUpdateEvent
    | CreateActionEvent
    | UpdateActionEvent
    | DeleteActionEvent
    | DatasetHealthFilterEvent
    | DatasetHealthClickEvent
    | WelcomeToDataHubModalViewEvent
    | WelcomeToDataHubModalInteractEvent
    | WelcomeToDataHubModalExitEvent
    | WelcomeToDataHubModalClickViewDocumentationEvent
    | ProductTourButtonClickEvent
    | IngestionTestConnectionEvent
    | IngestionExecutionResultViewedEvent
    | IngestionSourceConfigurationImpressionEvent
    | IngestionViewAllClickEvent
    | IngestionViewAllClickWarningEvent
    | SetDeprecationEvent
    | ClickInviteUsersCTAEvent
    | ClickCopyInviteLinkEvent
    | ClickInviteViaEmailEvent
    | ClickInviteRecommendedUserEvent
    | ClickBulkInviteRecommendedUsersEvent
    | ClickBulkDismissRecommendedUsersEvent
    | InviteUserErrorEvent
    | AssetPageAddSummaryElementEvent
    | AssetPageRemoveSummaryElementEvent
    | AssetPageReplaceSummaryElementEvent
    | FileUploadAttemptEvent
    | FileUploadFailedEvent
    | FileUploadSucceededEvent
    | FileDownloadViewEvent;
