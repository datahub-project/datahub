import { EntityType, RecommendationRenderType, ScenarioType } from '../../types.generated';

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
    BrowseResultClickEvent,
    HomePageBrowseResultClickEvent,
    EntityViewEvent,
    EntitySectionViewEvent,
    EntityActionEvent,
    BatchEntityActionEvent,
    RecommendationImpressionEvent,
    RecommendationClickEvent,
    HomePageRecommendationClickEvent,
    SearchAcrossLineageEvent,
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
    BatchSelectUserRoleEvent,
    CreatePolicyEvent,
    UpdatePolicyEvent,
    DeactivatePolicyEvent,
    ActivatePolicyEvent,
    ShowSimplifiedHomepageEvent,
    ShowStandardHomepageEvent,
    CreateGlossaryEntityEvent,
    CreateDomainEvent,
    CreateIngestionSourceEvent,
    UpdateIngestionSourceEvent,
    DeleteIngestionSourceEvent,
    ExecuteIngestionSourceEvent,
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
}

/**
 * Logged on user successful search query from the home page.
 */
export interface HomePageSearchEvent extends BaseEvent {
    type: EventType.HomePageSearchEvent;
    query: string;
    entityTypeFilter?: EntityType;
    pageNumber: number;
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
    renderId: string; // TODO : Determine whether we need a render id to join with click event.
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

export interface SearchAcrossLineageEvent extends BaseEvent {
    type: EventType.SearchAcrossLineageEvent;
    query: string;
    entityTypeFilter?: EntityType;
    pageNumber: number;
    originPath: string;
}
export interface SearchAcrossLineageResultsViewEvent extends BaseEvent {
    type: EventType.SearchAcrossLineageResultsViewEvent;
    query: string;
    entityTypeFilter?: EntityType;
    page?: number;
    total: number;
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

// Business glossary events

export interface CreateGlossaryEntityEvent extends BaseEvent {
    type: EventType.CreateGlossaryEntityEvent;
    entityType: EntityType;
    parentNodeUrn?: string;
}

export interface CreateDomainEvent extends BaseEvent {
    type: EventType.CreateDomainEvent;
}

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
    | SearchResultsViewEvent
    | SearchResultClickEvent
    | BrowseResultClickEvent
    | HomePageBrowseResultClickEvent
    | EntityViewEvent
    | EntitySectionViewEvent
    | EntityActionEvent
    | RecommendationImpressionEvent
    | SearchAcrossLineageEvent
    | SearchAcrossLineageResultsViewEvent
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
    | BatchSelectUserRoleEvent
    | CreatePolicyEvent
    | UpdatePolicyEvent
    | DeactivatePolicyEvent
    | ActivatePolicyEvent
    | ShowSimplifiedHomepageEvent
    | ShowStandardHomepageEvent
    | CreateGlossaryEntityEvent
    | CreateDomainEvent
    | CreateIngestionSourceEvent
    | UpdateIngestionSourceEvent
    | DeleteIngestionSourceEvent
    | ExecuteIngestionSourceEvent;
