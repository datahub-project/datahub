import { EntityType, RecommendationRenderType, ScenarioType } from '../../types.generated';

/**
 * Valid event types.
 */
export enum EventType {
    PageViewEvent,
    LogInEvent,
    LogOutEvent,
    SearchEvent,
    SearchResultsViewEvent,
    SearchResultClickEvent,
    EntitySearchResultClickEvent,
    BrowseResultClickEvent,
    EntityViewEvent,
    EntitySectionViewEvent,
    EntityActionEvent,
    RecommendationImpressionEvent,
    RecommendationClickEvent,
    SearchAcrossLineageEvent,
    SearchAcrossLineageResultsViewEvent,
    DownloadAsCsvEvent,
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
    entityType: EntityType;
    entityUrn: string;
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

/**
 * Event consisting of a union of specific event types.
 */
export type Event =
    | PageViewEvent
    | LogInEvent
    | LogOutEvent
    | SearchEvent
    | SearchResultsViewEvent
    | SearchResultClickEvent
    | BrowseResultClickEvent
    | EntityViewEvent
    | EntitySectionViewEvent
    | EntityActionEvent
    | RecommendationImpressionEvent
    | SearchAcrossLineageEvent
    | SearchAcrossLineageResultsViewEvent
    | DownloadAsCsvEvent
    | RecommendationClickEvent;
