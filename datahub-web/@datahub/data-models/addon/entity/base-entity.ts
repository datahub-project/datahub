import { IEntityRenderProps } from '@datahub/data-models/types/entity/rendering/entity-render-props';
import { IBaseEntity } from '@datahub/metadata-types/types/entity';
import { Snapshot } from '@datahub/metadata-types/types/metadata/snapshot';
import { computed } from '@ember/object';
import { MetadataAspect } from '@datahub/metadata-types/types/metadata/aspect';
import { getMetadataAspect } from '@datahub/metadata-types/constants/metadata/aspect';
import { IOwner } from '@datahub/metadata-types/types/common/owner';
import { map } from '@ember/object/computed';
import {
  IEntityLinkAttrs,
  IEntityLinkNode,
  IBrowsePath,
  IEntityLinkAttrsWithCount
} from '@datahub/data-models/types/entity/shared';
import { NotImplementedError } from '@datahub/data-models/constants/entity/shared/index';
import { readBrowse, readBrowsePath } from '@datahub/data-models/api/browse';
import { getFacetDefaultValueForEntity } from '@datahub/data-models/entity/utils/facets';
import { InstitutionalMemory } from '@datahub/data-models/models/aspects/institutional-memory';

/**
 * Interfaces and abstract classes define the "instance side" of a type / class,
 * therefore ts will only check the instance side
 * To check the static side / constructor, the interface should define the constructor, then static properties
 *
 * statics is a generic ClassDecorator that checks the static side T of the decorated class
 * @template T {T extends new (...args: Array<any>) => void} constrains T to constructor interfaces
 * @type {() => ClassDecorator}
 */
export const statics = <T extends new (...args: Array<unknown>) => void>(): ((c: T) => void) => (_ctor: T): void => {};

/**
 * Defines the interface for the static side or constructor of a class that extends BaseEntity<T>
 * @export
 * @interface IBaseEntityStatics
 * @template T constrained by the IBaseEntity interface, the entity interface that BaseEntity subclass will encapsulate
 */
export interface IBaseEntityStatics<T extends IBaseEntity> {
  new (urn: string): BaseEntity<T>;

  /**
   * Properties that guide the rendering of ui elements and features in the host application
   * @readonly
   * @static
   * @type {IEntityRenderProps}
   */
  renderProps: IEntityRenderProps;

  /**
   * Statically accessible name of the concrete DataModel type
   * @type {string}
   * @memberof IBaseEntityStatics
   */
  displayName: string;

  /**
   * Queries the entity's endpoint to retrieve the list of nodes that are contained in the hierarchy
   * @param {(...Array<string>)} args list of string values corresponding to the different hierarchical categories for the entity
   */
  readCategories(...args: Array<string>): Promise<IBrowsePath>;

  /**
   * Queries the entity's endpoint to get the count for categories. This call will be made if 'showCount' is true and
   * only will be available in the card layout.
   * @param args list fo segments in the entity hierarchy, maybe be culled from the entity, entity urn, or query (user entered url)
   */
  // TODO META-8863 remove once dataset is migrated
  readCategoriesCount(...segments: Array<string>): Promise<number>;

  /**
   * Queries the batch GET endpoint for snapshots for the supplied urns
   */
  readSnapshots(_urns: Array<string>): Promise<Array<Snapshot>>;

  /**
   * Builds a search query keyword from a list of segments for the related DataModelEntity
   * @memberof IBaseEntityStatics
   */
  getQueryForHierarchySegments(_segments: Array<string>): string;
}

/**
 * This defines the base attributes and methods for the instance side of an entity data model
 * i.e. the public interface for an entity object
 * Shared methods and default properties may be defined on this class
 * Properties intended to be implemented by the concrete entity, and shared methods that
 * need to be delegated to the concrete entity should be declared here
 * @export
 * @abstract
 * @class BaseEntity
 * @template T the entity interface that the entity model (subclass) encapsulates
 */
export abstract class BaseEntity<T extends IBaseEntity> {
  /**
   * A reference to the derived concrete entity instance
   * @type {T}
   */
  entity?: T;

  /**
   * References the Snapshot for the related Entity
   * @type {Snapshot}
   * @memberof BaseEntity
   */
  snapshot?: Snapshot;

  /**
   * References the wiki related documents and objects related to this entity
   * @memberof BaseEntity
   */
  institutionalMemories?: Array<InstitutionalMemory>;

  /**
   * A dictionary of host Ember application routes which can be used as route arguments to the link-to helper
   */
  get hostRoutes(): Record<string, string> {
    return { dataSourceRoute: 'datasets.dataset' };
  }

  /**
   * Indicates whether the entity has been removed (soft deletion), reads from reified entity,
   * otherwise defaults to false
   * @readonly
   * @type {boolean}
   * @memberof BaseEntity
   */
  @computed('entity')
  get removed(): boolean {
    return (this.entity && this.entity.removed) || false;
  }

  /**
   * Selects the list of owners from the ownership aspect attribute of the entity
   * All entities have an ownership aspect have this property exists on the base and is inherited
   *
   * This provides the full IOwnership objects in a list, if what is needed is just the list of
   * owner urns, the macro value ownerUrns provides that immediately
   * @readonly
   * @type {Array<IOwner>}
   * @memberof BaseEntity
   */
  @computed('snapshot')
  get owners(): Array<IOwner> {
    const ownership = getMetadataAspect(this.snapshot)(
      'com.linkedin.common.Ownership'
    ) as MetadataAspect['com.linkedin.common.Ownership'];

    return ownership ? ownership.owners : [];
  }

  /**
   * Extracts the owner urns into a list from each IOwner instance
   * @readonly
   * @type {Array<string>}
   * @memberof BaseEntity
   */
  @map('owners.[]', ({ owner }: IOwner): string => owner)
  ownerUrns!: Array<string>;

  /**
   * Class instance JSON serialization does not by default serialize non-enumerable property names
   * This provides a custom toJSON method to ensure that displayName attribute is present when serialized,
   * allowing correct de-serialization back to  source instance / DataModelEntity type
   */
  toJSON(): this {
    return { ...this, displayName: this.displayName };
  }

  /**
   * Statically accessible Base entity kind discriminant
   * @static
   * @memberof BaseEntity
   */
  static kind = 'BaseEntity';

  /**
   * Discriminant to allow the construction of discriminated / tagged union types
   * @type {string}
   */
  get kind(): string {
    // Expected to be implemented in concrete class
    throw new Error(NotImplementedError);
  }

  /**
   * Base entity display name
   * @static
   * @memberof BaseEntity
   */
  static displayName: string;

  /**
   * Human friendly string alias for the entity type e.g. Dashboard, Users
   * @type {string}
   */
  get displayName(): string {
    // Implemented in concrete class
    throw new Error(NotImplementedError);
  }

  /**
   * Workaround to get the current static instance
   * This makes sense if you want to get a static property only
   * implemented in a subclass, therefore, the same static
   * class is needed
   */
  get staticInstance(): IBaseEntityStatics<T> {
    return (this.constructor as unknown) as IBaseEntityStatics<T>;
  }

  /**
   * Will read the current path for an entity
   */
  get readPath(): Promise<Array<string>> {
    const entityName = this.staticInstance.renderProps.search.apiName;
    return readBrowsePath({
      type: entityName,
      urn: this.urn
    }).then(
      (paths): Array<string> => {
        return paths && paths.length > 0 ? paths[0].split('/').filter(Boolean) : [];
      }
    );
  }

  /**
   * Asynchronously resolves with an instance of T
   * @readonly
   * @type {Promise<T>}
   */
  get readEntity(): Promise<T> {
    // Implemented in concrete class
    throw new Error(NotImplementedError);
  }

  /**
   * Asynchronously resolves with the Snapshot for the entity
   * This should be implemented on the concrete class and is enforced to be available with the
   * abstract modifier
   * @readonly
   * @type {Promise<Snapshot>}
   */
  get readSnapshot(): Promise<Snapshot> {
    // Implemented in concrete class
    throw new Error(NotImplementedError);
  }

  /**
   * Class properties common across instances
   * Dictates how visual ui components should be rendered
   * Implemented as a getter to ensure that reads are idempotent
   */
  static get renderProps(): IEntityRenderProps {
    throw new Error(NotImplementedError);
  }

  /**
   * Read elements under provided by the browse endpoint for ump-metrics and group those elements by
   * specific keys on the IMetricsBrowse interface based on the properties returned by parsePrefix
   * @param {string} category the category the elements reside under in the hierarchy for ump_metrics
   * @param {string} [prefix] optional prefix string to constrain the browse results
   */
  static async readCategories(...segments: Array<string>): Promise<IBrowsePath> {
    const cleanSegments: Array<string> = segments.filter(Boolean) as Array<string>;
    const defaultFacets = this.renderProps.browse.attributes
      ? getFacetDefaultValueForEntity(this.renderProps.browse.attributes)
      : [];
    const { elements, metadata } = await readBrowse({
      type: this.renderProps.search.apiName,
      path: cleanSegments.length > 0 ? `/${cleanSegments.join('/')}` : '',
      count: 100,
      start: 0,
      ...defaultFacets
    });
    const entityLinks: Array<IEntityLinkAttrs> = elements.map(
      (element): IEntityLinkAttrs =>
        this.getLinkForEntity({
          displayName: element.name,
          entityUrn: element.urn
        })
    );
    const categoryLinks: Array<IEntityLinkAttrsWithCount> = metadata.groups.map(
      (group): IEntityLinkAttrsWithCount => {
        return this.getLinkForCategory({
          segments: [...cleanSegments, group.name],
          count: group.count,
          displayName: group.name
        });
      }
    );
    return {
      segments,
      title: segments[segments.length - 1] || this.displayName,
      count: metadata.totalNumEntities,
      entities: entityLinks,
      groups: categoryLinks
    };
  }

  /**
   * Will generate a link for an entity based on a displayName and a entityUrn
   */
  static getLinkForEntity(params: { entityUrn: string; displayName: string }): IEntityLinkAttrs {
    const { displayName, entityUrn } = params;
    const link: IEntityLinkNode = {
      title: displayName || '',
      text: displayName || '',
      route: this.renderProps.browse.entityRoute,
      model: [entityUrn || '']
    };

    return {
      link,
      entity: this.displayName
    };
  }

  /**
   * Generate link for category given segments, displayName and count as optional
   */
  static getLinkForCategory(params: {
    segments: Array<string>;
    displayName: string;
    count: number;
  }): IEntityLinkAttrsWithCount {
    const { segments, count, displayName } = params;
    const link: IEntityLinkNode = {
      title: displayName || '',
      text: displayName || segments[0] || '',
      route: 'browse.entity',
      model: [this.displayName],
      queryParams: { path: segments.filter(Boolean).join('/') }
    };
    return {
      link,
      entity: this.displayName,
      count
    };
  }

  // TODO META-8863 this can be removed once dataset is migrated
  static readCategoriesCount(..._args: Array<string>): Promise<number> {
    throw new Error(NotImplementedError);
  }

  /**
   * Reads the snapshots for the entity
   * @static
   */
  static readSnapshots(_urns: Array<string>): Promise<Array<Snapshot>> {
    throw new Error(NotImplementedError);
  }

  /**
   * Builds a search query keyword from a list of segments
   * @static
   * @param {Array<string>} [segments=[]] the list of hierarchy segments to generate the keyword for
   */
  static getQueryForHierarchySegments(segments: Array<string> = []): string {
    return `path:\\\\/${segments.join('\\\\/').replace(/\s/gi, '\\\\ ')}`;
  }

  /**
   * Retrieves a list of wiki documents related to the particular entity instance
   * @readonly
   */
  readInstitutionalMemory(): Promise<Array<InstitutionalMemory>> {
    throw new Error(NotImplementedError);
  }

  /**
   * Writes a list of wiki documents related to a particular entity instance to the api layer
   */
  writeInstitutionalMemory(): Promise<void> {
    throw new Error(NotImplementedError);
  }

  /**
   * Creates an instance of BaseEntity concrete class
   * @param {string} urn the urn for the entity being instantiated. urn is a parameter property
   * @memberof BaseEntity
   */
  constructor(readonly urn: string) {}
}
