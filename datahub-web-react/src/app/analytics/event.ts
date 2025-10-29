import { FileUploadFailureType } from '@components/components/Editor/types';

import { EmbedLookupNotFoundReason } from '@app/embed/lookup/constants';
import { PersonaType } from '@app/homeV2/shared/types';
import { Direction } from '@app/lineage/types';
import { FilterMode } from '@app/search/utils/constants';

import {
    AllowedValue,
    DataHubPageModuleType,
    DataHubViewType,
    EntityType,
    LineageDirection,
    PageTemplateSurfaceType,
    PropertyCardinality,
    PropertyValueInput,
    RecommendationRenderType,
    ResourceRefInput,
    ScenarioType,
    SearchBarApi,
    SummaryElementType,
    UploadDownloadScenario,
} from '@types';

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
    IntroduceYourselfViewEvent,
    IntroduceYourselfSubmitEvent,
    IntroduceYourselfSkipEvent,
    ExpandLineageEvent,
    ContractLineageEvent,
    ShowHideLineageColumnsEvent,
    SearchLineageColumnsEvent,
    FilterLineageColumnsEvent,
    DrillDownLineageEvent,
    NavBarExpandCollapse,
    NavBarItemClick,
    LinkAssetVersionEvent,
    UnlinkAssetVersionEvent,
    ShowAllVersionsEvent,
    HomePageClick,
    SearchBarFilter,
    FilterStatsPage,
    FilterStatsChartLookBack,
    ClickUserProfile,
    ClickViewDocumentation,
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
    SetDeprecation,
    WelcomeToDataHubModalViewEvent,
    WelcomeToDataHubModalInteractEvent,
    WelcomeToDataHubModalExitEvent,
    WelcomeToDataHubModalClickViewDocumentationEvent,
    ProductTourButtonClickEvent,
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
    actorUrn?: string;
    timestamp?: number;
    date?: string;
    userAgent?: string;
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
interface PageViewEvent extends BaseEvent {
    type: EventType.PageViewEvent;
    originPath: string;
}

/**
 * Viewed the Introduce Yourself page on the UI.
 */
interface IntroduceYourselfViewEvent extends BaseEvent {
    type: EventType.IntroduceYourselfViewEvent;
}

/**
 * Submitted the "Introduce Yourself" page through the UI.
 */
interface IntroduceYourselfSubmitEvent extends BaseEvent {
    type: EventType.IntroduceYourselfSubmitEvent;
    role: string;
    platformUrns: Array<string>;
}

/**
 * Skipped the "Introduce Yourself" page through the UI.
 */
interface IntroduceYourselfSkipEvent extends BaseEvent {
    type: EventType.IntroduceYourselfSkipEvent;
}

/**
 * Viewed the Home Page on the UI.
 */
interface HomePageViewEvent extends BaseEvent {
    type: EventType.HomePageViewEvent;
}

/**
 * Logged on successful new user sign up.
 */
interface SignUpEvent extends BaseEvent {
    type: EventType.SignUpEvent;
    title: string;
}

/**
 * Logged on user successful login.
 */
interface LogInEvent extends BaseEvent {
    type: EventType.LogInEvent;
}

/**
 * Logged on user successful logout.
 */
interface LogOutEvent extends BaseEvent {
    type: EventType.LogOutEvent;
}

/**
 * Logged on user resetting their credentials
 */
interface ResetCredentialsEvent extends BaseEvent {
    type: EventType.ResetCredentialsEvent;
}

/**
 * Logged on user successful search query.
 */
interface SearchEvent extends BaseEvent {
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
interface HomePageSearchEvent extends BaseEvent {
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
interface SearchResultsViewEvent extends BaseEvent {
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
interface SearchResultClickEvent extends BaseEvent {
    type: EventType.SearchResultClickEvent;
    query: string;
    entityUrn: string;
    entityType: EntityType;
    entityTypeFilter?: EntityType;
    index: number;
    total: number;
    pageNumber: number;
}

interface SearchFiltersClearAllEvent extends BaseEvent {
    type: EventType.SearchFiltersClearAllEvent;
    total: number;
}

interface SearchFiltersShowMoreEvent extends BaseEvent {
    type: EventType.SearchFiltersShowMoreEvent;
    activeFilterCount: number;
    hiddenFilterCount: number;
}

/**
 * Logged on user browse result click.
 */
interface BrowseResultClickEvent extends BaseEvent {
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
interface HomePageBrowseResultClickEvent extends BaseEvent {
    type: EventType.HomePageBrowseResultClickEvent;
    entityType: EntityType;
}

/**
 * Logged when a user opens or closes the browse v2 sidebar
 */
interface BrowseV2ToggleSidebarEvent extends BaseEvent {
    type: EventType.BrowseV2ToggleSidebarEvent;
    action: 'open' | 'close';
}

/**
 * Logged when a user opens or closes a sidebar node
 */
export interface BrowseV2ToggleNodeEvent extends BaseEvent {
    type: EventType.BrowseV2ToggleNodeEvent;
    targetNode: 'entity' | 'environment' | 'platform' | 'browse';
    action: 'open' | 'close';
    entity: string;
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
    entity: string;
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
    entity: string;
    environment?: string;
    platform?: string;
    targetDepth: number;
}

/**
 * Logged when user views an entity profile.
 */
interface EntityViewEvent extends BaseEvent {
    type: EventType.EntityViewEvent;
    entityType: EntityType;
    entityUrn: string;
}

/**
 * Logged when user views a particular section of an entity profile.
 */
interface EntitySectionViewEvent extends BaseEvent {
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
};

export enum ExternalLinkType {
    Custom = 'CUSTOM',
    Default = 'DEFAULT_EXTERNAL_URL',
}

interface EntityActionEvent extends BaseEvent {
    type: EventType.EntityActionEvent;
    actionType: string;
    entityType?: EntityType;
    entityUrn: string;
    externalLinkType?: ExternalLinkType;
}

interface BatchEntityActionEvent extends BaseEvent {
    type: EventType.BatchEntityActionEvent;
    actionType: string;
    entityUrns: string[];
}

interface RecommendationImpressionEvent extends BaseEvent {
    type: EventType.RecommendationImpressionEvent;
    moduleId: string;
    renderType: RecommendationRenderType;
    scenarioType: ScenarioType;
    // TODO: Determine whether we need to collect context parameters.
}

interface RecommendationClickEvent extends BaseEvent {
    type: EventType.RecommendationClickEvent;
    renderId: string; // TODO : Determine whether we need a render id to join with click event.
    moduleId: string;
    renderType: RecommendationRenderType;
    scenarioType: ScenarioType;
    index?: number;
}

interface HomePageRecommendationClickEvent extends BaseEvent {
    type: EventType.HomePageRecommendationClickEvent;
    renderId: string; // TODO : Determine whether we need a render id to join with click event.
    moduleId: string;
    renderType: RecommendationRenderType;
    scenarioType: ScenarioType;
    index?: number;
}

interface VisualLineageViewEvent extends BaseEvent {
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

interface VisualLineageExpandGraphEvent extends BaseEvent {
    type: EventType.VisualLineageExpandGraphEvent;
    targetEntityType?: EntityType;
}

interface SearchAcrossLineageEvent extends BaseEvent {
    type: EventType.SearchAcrossLineageEvent;
    query: string;
    entityTypeFilter?: EntityType;
    pageNumber: number;
    originPath: string;
    maxDegree?: string;
}
interface SearchAcrossLineageResultsViewEvent extends BaseEvent {
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

interface DownloadAsCsvEvent extends BaseEvent {
    type: EventType.DownloadAsCsvEvent;
    query: string;
    // optional parameter if its coming from inside an entity page
    entityUrn?: string;
    path: string;
}

interface CreateAccessTokenEvent extends BaseEvent {
    type: EventType.CreateAccessTokenEvent;
    accessTokenType: string;
    duration: string;
}

interface RevokeAccessTokenEvent extends BaseEvent {
    type: EventType.RevokeAccessTokenEvent;
}

interface CreateGroupEvent extends BaseEvent {
    type: EventType.CreateGroupEvent;
}
interface CreateInviteLinkEvent extends BaseEvent {
    type: EventType.CreateInviteLinkEvent;
    roleUrn?: string;
}

interface CreateResetCredentialsLinkEvent extends BaseEvent {
    type: EventType.CreateResetCredentialsLinkEvent;
    userUrn: string;
}

interface DeleteEntityEvent extends BaseEvent {
    type: EventType.DeleteEntityEvent;
    entityUrn: string;
    entityType: EntityType;
}

interface SelectUserRoleEvent extends BaseEvent {
    type: EventType.SelectUserRoleEvent;
    roleUrn: string;
    userUrn: string;
}

interface SelectGroupRoleEvent extends BaseEvent {
    type: EventType.SelectGroupRoleEvent;
    roleUrn: string;
    groupUrn?: string;
}

interface BatchSelectUserRoleEvent extends BaseEvent {
    type: EventType.BatchSelectUserRoleEvent;
    roleUrn: string;
    userUrns: string[];
}

// Policy events

interface CreatePolicyEvent extends BaseEvent {
    type: EventType.CreatePolicyEvent;
}

interface UpdatePolicyEvent extends BaseEvent {
    type: EventType.UpdatePolicyEvent;
    policyUrn: string;
}

interface DeactivatePolicyEvent extends BaseEvent {
    type: EventType.DeactivatePolicyEvent;
    policyUrn: string;
}

interface ActivatePolicyEvent extends BaseEvent {
    type: EventType.ActivatePolicyEvent;
    policyUrn: string;
}

interface ShowSimplifiedHomepageEvent extends BaseEvent {
    type: EventType.ShowSimplifiedHomepageEvent;
}

interface ShowStandardHomepageEvent extends BaseEvent {
    type: EventType.ShowStandardHomepageEvent;
}

interface ShowV2ThemeEvent extends BaseEvent {
    type: EventType.ShowV2ThemeEvent;
}

interface RevertV2ThemeEvent extends BaseEvent {
    type: EventType.RevertV2ThemeEvent;
}

interface HomePageExploreAllClickEvent extends BaseEvent {
    type: EventType.HomePageExploreAllClickEvent;
}

interface SearchBarExploreAllClickEvent extends BaseEvent {
    type: EventType.SearchBarExploreAllClickEvent;
}

interface SearchResultsExploreAllClickEvent extends BaseEvent {
    type: EventType.SearchResultsExploreAllClickEvent;
}

// Business glossary events

interface CreateGlossaryEntityEvent extends BaseEvent {
    type: EventType.CreateGlossaryEntityEvent;
    entityType: EntityType;
    parentNodeUrn?: string;
}

interface CreateDomainEvent extends BaseEvent {
    type: EventType.CreateDomainEvent;
    parentDomainUrn?: string;
}

interface MoveDomainEvent extends BaseEvent {
    type: EventType.MoveDomainEvent;
    oldParentDomainUrn?: string;
    parentDomainUrn?: string;
}

// Managed Ingestion Events

interface IngestionTestConnectionEvent extends BaseEvent {
    type: EventType.IngestionTestConnectionEvent;
    sourceType: string;
    sourceUrn?: string;
    outcome?: string;
}

interface IngestionViewAllClickEvent extends BaseEvent {
    type: EventType.IngestionViewAllClickEvent;
    executionUrn?: string;
}

interface IngestionViewAllClickWarningEvent extends BaseEvent {
    type: EventType.IngestionViewAllClickWarningEvent;
    executionUrn?: string;
}

interface IngestionExecutionResultViewedEvent extends BaseEvent {
    type: EventType.IngestionExecutionResultViewedEvent;
    executionUrn: string;
    executionStatus: string;
    viewedSection: string;
}

interface IngestionSourceConfigurationImpressionEvent extends BaseEvent {
    type: EventType.IngestionSourceConfigurationImpressionEvent;
    viewedSection: 'SELECT_TEMPLATE' | 'DEFINE_RECIPE' | 'CREATE_SCHEDULE' | 'NAME_SOURCE';
    sourceType?: string;
    sourceUrn?: string;
}

interface CreateIngestionSourceEvent extends BaseEvent {
    type: EventType.CreateIngestionSourceEvent;
    sourceType: string;
    sourceUrn?: string;
    interval?: string;
    numOwners?: number;
    outcome?: string;
}

interface UpdateIngestionSourceEvent extends BaseEvent {
    type: EventType.UpdateIngestionSourceEvent;
    sourceType: string;
    sourceUrn: string;
    interval?: string;
    numOwners?: number;
    outcome?: string;
}

interface DeleteIngestionSourceEvent extends BaseEvent {
    type: EventType.DeleteIngestionSourceEvent;
}

interface ExecuteIngestionSourceEvent extends BaseEvent {
    type: EventType.ExecuteIngestionSourceEvent;
    sourceType?: string;
    sourceUrn?: string;
}

// TODO: Find a way to use this event
interface SsoEvent extends BaseEvent {
    type: EventType.SsoEvent;
}

interface ManuallyCreateLineageEvent extends BaseEvent {
    type: EventType.ManuallyCreateLineageEvent;
    direction: Direction;
    sourceEntityType?: EntityType;
    sourceEntityPlatform?: string;
    destinationEntityType?: EntityType;
    destinationEntityPlatform?: string;
}

interface ManuallyDeleteLineageEvent extends BaseEvent {
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
interface CreateViewEvent extends BaseEvent {
    type: EventType.CreateViewEvent;
    viewType?: DataHubViewType;
    filterFields: string[];
    entityTypes: string[];
    searchVersion: string;
}

/**
 * Emitted when an existing View is updated.
 */
interface UpdateViewEvent extends BaseEvent {
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
interface SetUserDefaultViewEvent extends BaseEvent {
    type: EventType.SetUserDefaultViewEvent;
    urn: string | null;
    viewType: DataHubViewType | null;
}

/**
 * Emitted when a user sets or clears the global default view.
 */
interface SetGlobalDefaultViewEvent extends BaseEvent {
    type: EventType.SetGlobalDefaultViewEvent;
    urn: string | null;
}

interface LineageGraphTimeRangeSelectionEvent extends BaseEvent {
    type: EventType.LineageGraphTimeRangeSelectionEvent;
    relativeStartDate: string;
    relativeEndDate: string;
}

interface LineageTabTimeRangeSelectionEvent extends BaseEvent {
    type: EventType.LineageTabTimeRangeSelectionEvent;
    relativeStartDate: string;
    relativeEndDate: string;
}

interface CreateQueryEvent extends BaseEvent {
    type: EventType.CreateQueryEvent;
}

interface UpdateQueryEvent extends BaseEvent {
    type: EventType.UpdateQueryEvent;
}

interface DeleteQueryEvent extends BaseEvent {
    type: EventType.DeleteQueryEvent;
}

interface SelectAutoCompleteOption extends BaseEvent {
    type: EventType.SelectAutoCompleteOption;
    optionType: string;
    entityType?: EntityType;
    entityUrn?: string;
    showSearchBarAutocompleteRedesign?: boolean;
    apiVariant?: SearchBarApi;
}

interface SelectQuickFilterEvent extends BaseEvent {
    type: EventType.SelectQuickFilterEvent;
    quickFilterType: string;
    quickFilterValue: string;
}

interface DeselectQuickFilterEvent extends BaseEvent {
    type: EventType.DeselectQuickFilterEvent;
    quickFilterType: string;
    quickFilterValue: string;
}

interface EmbedProfileViewEvent extends BaseEvent {
    type: EventType.EmbedProfileViewEvent;
    entityType: string;
    entityUrn: string;
}

interface EmbedProfileViewInDataHubEvent extends BaseEvent {
    type: EventType.EmbedProfileViewInDataHubEvent;
    entityType: string;
    entityUrn: string;
}

interface EmbedLookupNotFoundEvent extends BaseEvent {
    type: EventType.EmbedLookupNotFoundEvent;
    url: string;
    reason: EmbedLookupNotFoundReason;
}

interface CreateBusinessAttributeEvent extends BaseEvent {
    type: EventType.CreateBusinessAttributeEvent;
    name: string;
}

export enum DocRequestCTASource {
    AssetPage = 'AssetPage',
}

interface ClickDocRequestCTA extends BaseEvent {
    type: EventType.ClickDocRequestCTA;
    source: DocRequestCTASource;
}

interface ExpandLineageEvent extends BaseEvent {
    type: EventType.ExpandLineageEvent;
    direction: LineageDirection;
    levelsExpanded: '1' | 'all';
    entityUrn: string;
    entityType: EntityType;
}

interface ContractLineageEvent extends BaseEvent {
    type: EventType.ContractLineageEvent;
    direction: LineageDirection;
    entityUrn: string;
    entityType?: EntityType;
}

interface ShowHideLineageColumnsEvent extends BaseEvent {
    type: EventType.ShowHideLineageColumnsEvent;
    action: 'show' | 'hide';
    entityUrn: string;
    entityType: EntityType;
    entityPlatformUrn?: string;
}

interface SearchLineageColumnsEvent extends BaseEvent {
    type: EventType.SearchLineageColumnsEvent;
    entityUrn: string;
    entityType: EntityType;
    searchTextLength: number;
}

interface FilterLineageColumnsEvent extends BaseEvent {
    type: EventType.FilterLineageColumnsEvent;
    action: 'enable' | 'disable';
    entityUrn: string;
    entityType: EntityType;
    shownCount: number;
}

interface DrillDownLineageEvent extends BaseEvent {
    type: EventType.DrillDownLineageEvent;
    action: 'select' | 'deselect';
    entityUrn: string;
    entityType: EntityType;
    parentUrn: string;
    parentEntityType: EntityType;
    dataType?: string;
}

interface CreateStructuredPropertyClickEvent extends BaseEvent {
    type: EventType.CreateStructuredPropertyClickEvent;
}

interface StructuredPropertyEvent extends BaseEvent {
    propertyType: string;
    appliesTo: string[];
    qualifiedName?: string;
    allowedAssetTypes?: string[];
    allowedValues?: AllowedValue[];
    cardinality?: PropertyCardinality;
    showInFilters?: boolean;
    isHidden: boolean;
    showInSearchFilters: boolean;
    showAsAssetBadge: boolean;
    showInAssetSummary: boolean;
    hideInAssetSummaryWhenEmpty: boolean;
    showInColumnsTable: boolean;
}

interface CreateStructuredPropertyEvent extends StructuredPropertyEvent {
    type: EventType.CreateStructuredPropertyEvent;
}

interface EditStructuredPropertyEvent extends StructuredPropertyEvent {
    type: EventType.EditStructuredPropertyEvent;
    propertyUrn: string;
}

interface DeleteStructuredPropertyEvent extends StructuredPropertyEvent {
    type: EventType.DeleteStructuredPropertyEvent;
    propertyUrn: string;
}

interface ViewStructuredPropertyEvent extends BaseEvent {
    type: EventType.ViewStructuredPropertyEvent;
    propertyUrn: string;
}

interface StructuredPropertyOnAssetEvent extends BaseEvent {
    propertyUrn: string;
    propertyType: string;
    assetUrn: string;
    assetType: EntityType;
}
interface ApplyStructuredPropertyEvent extends StructuredPropertyOnAssetEvent {
    type: EventType.ApplyStructuredPropertyEvent;
    values: PropertyValueInput[];
}

interface UpdateStructuredPropertyOnAssetEvent extends StructuredPropertyOnAssetEvent {
    type: EventType.UpdateStructuredPropertyOnAssetEvent;
    values: PropertyValueInput[];
}

interface RemoveStructuredPropertyEvent extends StructuredPropertyOnAssetEvent {
    type: EventType.RemoveStructuredPropertyEvent;
}

interface LinkAssetVersionEvent extends BaseEvent {
    type: EventType.LinkAssetVersionEvent;
    newAssetUrn: string;
    oldAssetUrn?: string;
    versionSetUrn?: string;
    entityType: EntityType;
}

interface UnlinkAssetVersionEvent extends BaseEvent {
    type: EventType.UnlinkAssetVersionEvent;
    assetUrn: string;
    versionSetUrn?: string;
    entityType: EntityType;
}

interface ShowAllVersionsEvent extends BaseEvent {
    type: EventType.ShowAllVersionsEvent;
    assetUrn: string;
    versionSetUrn?: string;
    entityType: EntityType;
    numVersions?: number;
    uiLocation: 'preview' | 'more-options';
}

interface ClickUserProfileEvent extends BaseEvent {
    type: EventType.ClickUserProfile;
    location?: 'statsTabTopUsers'; // add more locations here
}

interface ClickViewDocumentationEvent extends BaseEvent {
    type: EventType.ClickViewDocumentation;
    link: string;
    location: 'statsTab'; // add more locations here
}

export enum HomePageModule {
    YouRecentlyViewed = 'YouRecentlyViewed',
    Discover = 'Discover',
    Announcements = 'Announcements',
    PersonalSidebar = 'PersonalSidebar',
    }

interface HomePageClickEvent extends BaseEvent {
    type: EventType.HomePageClick;
    module: HomePageModule;
    section?: string;
    subSection?: string;
    value?: string; // what was actually clicked ie. an entity urn to go to a page, or "View all" for a section
}

interface SearchBarFilterEvent extends BaseEvent {
    type: EventType.SearchBarFilter;
    field: string; // the filter field
    values: string[]; // the values being filtered for
}

interface NavBarExpandCollapseEvent extends BaseEvent {
    type: EventType.NavBarExpandCollapse;
    isExpanding: boolean; // whether this action is expanding or collapsing the nav bar
}

interface NavBarItemClickEvent extends BaseEvent {
    type: EventType.NavBarItemClick;
    label: string; // the label of the item that is clicks from the nav sidebar
}

interface FilterStatsPageEvent extends BaseEvent {
    type: EventType.FilterStatsPage;
    platform: string | null;
}

interface FilterStatsChartLookBackEvent extends BaseEvent {
    type: EventType.FilterStatsChartLookBack;
    lookBackValue: string;
    chartName: string;
}

interface WelcomeToDataHubModalViewEvent extends BaseEvent {
    type: EventType.WelcomeToDataHubModalViewEvent;
}

interface WelcomeToDataHubModalInteractEvent extends BaseEvent {
    type: EventType.WelcomeToDataHubModalInteractEvent;
    currentSlide: number;
    totalSlides: number;
}

interface WelcomeToDataHubModalExitEvent extends BaseEvent {
    type: EventType.WelcomeToDataHubModalExitEvent;
    currentSlide: number;
    totalSlides: number;
    exitMethod: 'close_button' | 'get_started_button' | 'outside_click' | 'escape_key';
}

interface WelcomeToDataHubModalClickViewDocumentationEvent extends BaseEvent {
    type: EventType.WelcomeToDataHubModalClickViewDocumentationEvent;
    url: string;
}

interface ProductTourButtonClickEvent extends BaseEvent {
    type: EventType.ProductTourButtonClickEvent;
    originPage: string; // Page where the button was clicked
}

interface ClickProductUpdateEvent extends BaseEvent {
    type: EventType.ClickProductUpdate;
    id: string;
    url: string;
}

interface HomePageTemplateModuleCreateEvent extends BaseEvent {
    type: EventType.HomePageTemplateModuleCreate;
    templateUrn: string;
    isPersonal: boolean;
    moduleType: DataHubPageModuleType;
    location: PageTemplateSurfaceType;
}

interface HomePageTemplateModuleAddEvent extends BaseEvent {
    type: EventType.HomePageTemplateModuleAdd;
    templateUrn: string;
    isPersonal: boolean;
    moduleType: DataHubPageModuleType;
    location: PageTemplateSurfaceType;
}

interface HomePageTemplateModuleUpdateEvent extends BaseEvent {
    type: EventType.HomePageTemplateModuleUpdate;
    templateUrn: string;
    isPersonal: boolean;
    moduleType: DataHubPageModuleType;
    location: PageTemplateSurfaceType;
}

interface HomePageTemplateModuleDeleteEvent extends BaseEvent {
    type: EventType.HomePageTemplateModuleDelete;
    templateUrn: string;
    isPersonal: boolean;
    moduleType: DataHubPageModuleType;
    location: PageTemplateSurfaceType;
}

interface HomePageTemplateModuleMoveEvent extends BaseEvent {
    type: EventType.HomePageTemplateModuleMove;
    templateUrn: string;
    isPersonal: boolean;
    location: PageTemplateSurfaceType;
}

interface HomePageTemplateModuleModalCreateOpenEvent extends BaseEvent {
    type: EventType.HomePageTemplateModuleModalCreateOpen;
    moduleType: DataHubPageModuleType;
    location: PageTemplateSurfaceType;
}

interface HomePageTemplateModuleModalEditOpenEvent extends BaseEvent {
    type: EventType.HomePageTemplateModuleModalEditOpen;
    moduleType: DataHubPageModuleType;
    location: PageTemplateSurfaceType;
}

interface HomePageTemplateModuleModalCancelEvent extends BaseEvent {
    type: EventType.HomePageTemplateModuleModalCancel;
    moduleType: DataHubPageModuleType;
    location: PageTemplateSurfaceType;
}

interface HomePageTemplateGlobalTemplateEditingStartEvent extends BaseEvent {
    type: EventType.HomePageTemplateGlobalTemplateEditingStart;
}

interface HomePageTemplateGlobalTemplateEditingDoneEvent extends BaseEvent {
    type: EventType.HomePageTemplateGlobalTemplateEditingDone;
}

interface HomePageTemplateResetToGlobalTemplateEvent extends BaseEvent {
    type: EventType.HomePageTemplateResetToGlobalTemplate;
}

interface HomePageTemplateModuleAssetClickEvent extends BaseEvent {
    type: EventType.HomePageTemplateModuleAssetClick;
    moduleType: DataHubPageModuleType;
    assetUrn: string;
    location: PageTemplateSurfaceType;
}

interface HomePageTemplateModuleExpandClickEvent extends BaseEvent {
    type: EventType.HomePageTemplateModuleExpandClick;
    moduleType: DataHubPageModuleType;
    assetUrn: string;
    location: PageTemplateSurfaceType;
}

interface HomePageTemplateModuleViewAllClickEvent extends BaseEvent {
    type: EventType.HomePageTemplateModuleViewAllClick;
    moduleType: DataHubPageModuleType;
    location: PageTemplateSurfaceType;
}

interface HomePageTemplateModuleLinkClickEvent extends BaseEvent {
    type: EventType.HomePageTemplateModuleLinkClick;
    link: string;
}

interface HomePageTemplateModuleAnnouncementDismissEvent extends BaseEvent {
    type: EventType.HomePageTemplateModuleAnnouncementDismiss;
}

interface SetDeprecationEvent extends BaseEvent {
    type: EventType.SetDeprecation;
    entityUrns: string[];
    deprecated: boolean;
    resources?: ResourceRefInput[];
}

interface AssetPageAddSummaryElementEvent extends BaseEvent {
    type: EventType.AssetPageAddSummaryElement;
    templateUrn: string;
    elementType: SummaryElementType;
}

interface AssetPageRemoveSummaryElementEvent extends BaseEvent {
    type: EventType.AssetPageRemoveSummaryElement;
    templateUrn: string;
    elementType: SummaryElementType;
}

interface AssetPageReplaceSummaryElementEvent extends BaseEvent {
    type: EventType.AssetPageReplaceSummaryElement;
    templateUrn: string;
    currentElementType: SummaryElementType;
    newElementType: SummaryElementType;
}

interface FileUploadAttemptEvent extends BaseEvent {
    type: EventType.FileUploadAttemptEvent;
    fileType: string;
    fileSize: number;
    scenario: UploadDownloadScenario;
    source: 'drag-and-drop' | 'button';
    assetUrn?: string;
    schemaFieldUrn?: string;
}

interface FileUploadFailedEvent extends BaseEvent {
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

interface FileUploadSucceededEvent extends BaseEvent {
    type: EventType.FileUploadSucceededEvent;
    fileType: string;
    fileSize: number;
    scenario: UploadDownloadScenario;
    source: 'drag-and-drop' | 'button';
    assetUrn?: string;
    schemaFieldUrn?: string;
}

interface FileDownloadViewEvent extends BaseEvent {
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
    | CreateAccessTokenEvent
    | RevokeAccessTokenEvent
    | CreateGroupEvent
    | CreateInviteLinkEvent
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
    | ShowStandardHomepageEvent
    | ShowV2ThemeEvent
    | RevertV2ThemeEvent
    | SsoEvent
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
    | ExpandLineageEvent
    | ContractLineageEvent
    | ShowHideLineageColumnsEvent
    | SearchLineageColumnsEvent
    | FilterLineageColumnsEvent
    | DrillDownLineageEvent
    | CreateStructuredPropertyClickEvent
    | CreateStructuredPropertyEvent
    | EditStructuredPropertyEvent
    | DeleteStructuredPropertyEvent
    | ViewStructuredPropertyEvent
    | ApplyStructuredPropertyEvent
    | UpdateStructuredPropertyOnAssetEvent
    | RemoveStructuredPropertyEvent
    | ClickDocRequestCTA
    | LinkAssetVersionEvent
    | UnlinkAssetVersionEvent
    | ShowAllVersionsEvent
    | NavBarExpandCollapseEvent
    | NavBarItemClickEvent
    | HomePageClickEvent
    | SearchBarFilterEvent
    | FilterStatsPageEvent
    | FilterStatsChartLookBackEvent
    | ClickUserProfileEvent
    | ClickViewDocumentationEvent
    | ClickProductUpdateEvent
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
    | SetDeprecationEvent
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
    | AssetPageAddSummaryElementEvent
    | AssetPageRemoveSummaryElementEvent
    | AssetPageReplaceSummaryElementEvent
    | FileUploadAttemptEvent
    | FileUploadFailedEvent
    | FileUploadSucceededEvent
    | FileDownloadViewEvent;
