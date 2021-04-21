import { EntityType } from '../../types.generated';

/**
 * Valid event types.
 */
export enum EventType {
    LogInEvent,
    LogOutEvent,
    SearchEvent,
    SearchResultClickEvent,
    BrowseResultClickEvent,
    EntityViewEvent,
    EntitySectionViewEvent,
    EntityActionEvent,
}

/**
 * Base Interface for all React analytics events.
 */
export interface BaseEvent {
    type: EventType;
    actor?: string;
    timestamp?: number;
    urn?: string;
    date?: string;
    userAgent?: string;
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
export interface SearchResultClickEvent extends BaseEvent {
    type: EventType.SearchResultClickEvent;
    query: string;
    urn: string;
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
    urn?: string;
    name?: string;
}

/**
 * Logged when user views an entity profile.
 */
export interface EntityViewEvent extends BaseEvent {
    type: EventType.EntityViewEvent;
    entityType: EntityType;
    urn: string;
}

/**
 * Logged when user views a particular section of an entity profile.
 */
export interface EntitySectionViewEvent extends BaseEvent {
    type: EventType.EntitySectionViewEvent;
    entityType: EntityType;
    urn: string;
    section: string;
}

/**
 * Logged when user interacts with a particular control of an entity.
 */
export interface EntityActionEvent extends BaseEvent {
    type: EventType.EntityActionEvent;
    entityType: EntityType;
    urn: string;
    action: string;
}

/**
 * Event consisting of a union of specific event types.
 */
export type Event =
    | LogInEvent
    | LogOutEvent
    | SearchEvent
    | SearchResultClickEvent
    | BrowseResultClickEvent
    | EntityViewEvent
    | EntitySectionViewEvent
    | EntityActionEvent;
