import { BaseEntity } from '@datahub/data-models/entity/base-entity';
import { IBaseEntity } from '@datahub/metadata-types/types/entity';
import { IDynamicLinkNode } from '@datahub/utils/types/vendor/dynamic-link';

/**
 * String literal of available entity routes
 */
export type AppRoute = 'browse.entity' | 'datasets.dataset' | 'user.profile';

/**
 * Properties that enable a dynamic link to the entity or category
 * @template T {T = unknown} by default, queryParams for EntityLinkNode are not specified, however, otherwise an associated type must be provided
 */
export type EntityLinkNode<T = unknown> = IDynamicLinkNode<Array<string>, AppRoute, T>;

/**
 * Envelopes the link: EntityLinkNode with metadata on how the link should be displayed
 * @export
 * @interface IEntityLinkAttrs
 * @template T {T = unknown} by default, queryParams for IEntityLinkAttrs are not specified, however, otherwise an associated type must be provided
 */
export interface IEntityLinkAttrs<T = unknown> {
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
  // count of total entities under this path
  count?: number;
  // link to groups/folders
  groups: Array<IEntityLinkAttrsWithCount>;
  // link to entities
  entities: Array<IEntityLinkAttrs>;
}
