import { EntityType } from '../../types.generated';

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
    BrowseResultClickEvent,
    EntityViewEvent,
    EntitySectionViewEvent,
    EntityActionEvent,
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
    UpdateOwnership: 'UpdateOwnership',
    UpdateDocumentation: 'UpdateDocumentation',
    UpdateDescription: 'UpdateDescription',
    UpdateSchemaDescription: 'UpdateSchemaDescription',
    UpdateSchemaTags: 'UpdateSchemaTags',
    ClickExternalUrl: 'ClickExternalUrl',
};

export interface EntityActionEvent extends BaseEvent {
    type: EventType.EntityActionEvent;
    actionType: string;
    entityType: EntityType;
    entityUrn: string;
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
    | EntityActionEvent;
