import { BaseEntity } from '@datahub/data-models/entity/base-entity';
import { IBaseEntity } from '@datahub/metadata-types/types/entity';

/**
 * String literal of available entity routes
 */
export type EntityRoute = string;

/**
 * Properties that enable a dynamic link to the entity or category
 * @export
 * @interface IEntityLinkNode
 */
export interface IEntityLinkNode<T = unknown> {
  // Title of the entity or category
  title: string;
  // Display value for the link
  text: string;
  // Route to navigate to the entity or category
  route: EntityRoute;
  // Segments values used in constructing the link-to route for the EntityRoute
  model: Array<string>;
  // Query parameters for the EntityRoute
  queryParams?: T;
}

/**
 * Envelopes the link: IEntityLinkNode with metadata on how the link should be displayed
 * @export
 * @interface IEntityLinkAttrs
 */
export interface IEntityLinkAttrs<T = unknown> {
  // Link properties that enable a dynamic link tho the entity or category
  link: IEntityLinkNode<T>;
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
