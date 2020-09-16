import { BaseEntity } from '@datahub/data-models/entity/base-entity';
import { IBaseEntity } from '@datahub/metadata-types/types/entity';
import { IDynamicLinkNode } from '@datahub/utils/types/vendor/dynamic-link';
import { DataModelName } from '@datahub/data-models/constants/entity';

/**
 * String literal of available entity routes
 */
export type AppRoute =
  | 'browse.entity'
  | 'features.feature'
  | 'datasets.dataset'
  | 'user.profile'
  | 'dataconcepts.dataconcept'
  | 'jobs.job'
  | EntityPageRoute;

/**
 * Specifies the top level Ember route representing navigation to an instance of entity page
 */
export type EntityPageRoute = 'entity-type.urn';

/**
 * Properties that enable a dynamic link to the entity or category
 * @template T {T = undefined} by default, queryParams for EntityLinkNode are not specified, however, otherwise an associated type must be provided
 */
export type EntityLinkNode<T = undefined> = IDynamicLinkNode<Array<string>, AppRoute, T>;

/**
 * Search Link type alias
 */
export type SearchLinkNode = IDynamicLinkNode<
  Array<void>,
  'search',
  { entity: DataModelName; keyword: string; page?: number }
>;

/**
 * Envelopes the link: EntityLinkNode with metadata on how the link should be displayed
 * @export
 * @interface IEntityLinkAttrs
 * @template T {T = undefined} by default, queryParams for IEntityLinkAttrs are not specified, however, otherwise an associated type must be provided
 */
export interface IEntityLinkAttrs<T = undefined> {
  // Link properties that enable a dynamic link tho the entity or category
  link: EntityLinkNode<T>;
  // Display name for the concrete entity class
  entity: BaseEntity<IBaseEntity>['displayName'];
}

/**
 * For groups we have a count in the link
 */
export interface IEntityLinkAttrsWithCount<T = unknown> extends IEntityLinkAttrs<T> {
  count: number;
}

/**
 * Browse Path represents a path/category when browsing. It can contains groups
 * and entities.
 */
export interface IBrowsePath {
  // The title for this path (usually last part of the path)
  title: string;
  // segments of the path (path separated into an array of string)
  segments: Array<string>;
  // count of total entities under this path (nested)
  totalNumEntities?: number;
  // number of entities only in current path
  entitiesPaginationCount: number;
  // link to groups/folders
  groups: Array<IEntityLinkAttrsWithCount>;
  // link to entities
  entities: Array<IEntityLinkAttrs>;
}
