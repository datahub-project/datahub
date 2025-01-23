import {
    AllowedValue,
    DataHubViewType,
    EntityType,
    PropertyCardinality,
    PropertyValueInput,
    RecommendationRenderType,
    ScenarioType,
} from '../../types.generated';
import { EmbedLookupNotFoundReason } from '../embed/lookup/constants';
import { Direction } from '../lineage/types';
import { FilterMode } from '../search/utils/constants';

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
    CreateGlossaryEntityEvent,
    CreateDomainEvent,
    MoveDomainEvent,
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
}

/**
 * Viewed a page on the UI.
 */
export interface PageViewEvent extends BaseEvent {
    type: EventType.PageViewEvent;
    originPath: string;
}

/**
 * Viewed the Home Page on the UI.
 */
export interface HomePageViewEvent extends BaseEvent {
    type: EventType.HomePageViewEvent;
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
    UpdateOwnership: 'UpdateOwnership',
    UpdateDocumentation: 'UpdateDocumentation',
    UpdateDescription: 'UpdateDescription',
    UpdateProperties: 'UpdateProperties',
    UpdateSchemaDescription: 'UpdateSchemaDescription',
    UpdateSchemaTags: 'UpdateSchemaTags',
    UpdateSchemaTerms: 'UpdateSchemaTerms',
    ClickExternalUrl: 'ClickExternalUrl',
    AddIncident: 'AddIncident',
    ResolvedIncident: 'ResolvedIncident',
};
export interface EntityActionEvent extends BaseEvent {
    type: EventType.EntityActionEvent;
    actionType: string;
    entityType?: EntityType;
    entityUrn: string;
}

export interface BatchEntityActionEvent extends BaseEvent {
    type: EventType.BatchEntityActionEvent;
    actionType: string;
    entityUrns: string[];
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
    entityType?: EntityType;
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

export interface CreateIngestionSourceEvent extends BaseEvent {
    type: EventType.CreateIngestionSourceEvent;
    sourceType: string;
    interval?: string;
}

export interface UpdateIngestionSourceEvent extends BaseEvent {
    type: EventType.UpdateIngestionSourceEvent;
    sourceType: string;
    interval?: string;
}

export interface DeleteIngestionSourceEvent extends BaseEvent {
    type: EventType.DeleteIngestionSourceEvent;
}

export interface ExecuteIngestionSourceEvent extends BaseEvent {
    type: EventType.ExecuteIngestionSourceEvent;
}

// TODO: Find a way to use this event
export interface SsoEvent extends BaseEvent {
    type: EventType.SsoEvent;
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
    showInFilters?: boolean;
    isHidden: boolean;
    showInSearchFilters: boolean;
    showAsAssetBadge: boolean;
    showInAssetSummary: boolean;
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

export interface RemoveStructuredPropertyEvent extends StructuredPropertyOnAssetEvent {
    type: EventType.RemoveStructuredPropertyEvent;
}

/**
 * Event consisting of a union of specific event types.
 */
export type Event =
    | PageViewEvent
    | HomePageViewEvent
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
    | CreateStructuredPropertyClickEvent
    | CreateStructuredPropertyEvent
    | EditStructuredPropertyEvent
    | DeleteStructuredPropertyEvent
    | ViewStructuredPropertyEvent
    | ApplyStructuredPropertyEvent
    | UpdateStructuredPropertyOnAssetEvent
    | RemoveStructuredPropertyEvent;
