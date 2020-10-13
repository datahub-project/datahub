import { IEntityRenderProps } from '@datahub/data-models/types/entity/rendering/entity-render-props';
import { Snapshot } from '@datahub/metadata-types/types/metadata/snapshot';
import { computed, set } from '@ember/object';
import { MetadataAspect } from '@datahub/metadata-types/types/metadata/aspect';
import { getMetadataAspect } from '@datahub/metadata-types/constants/metadata/aspect';
import { map, mapBy } from '@ember/object/computed';
import {
  IEntityLinkAttrs,
  EntityLinkNode,
  IBrowsePath,
  IEntityLinkAttrsWithCount,
  AppRoute,
  EntityPageRoute
} from '@datahub/data-models/types/entity/shared';
import { NotImplementedError } from '@datahub/data-models/constants/entity/shared/index';
import { readBrowse, readBrowsePath } from '@datahub/data-models/api/browse';
import { getFacetDefaultValueForEntity } from '@datahub/data-models/entity/utils/facets';
import { InstitutionalMemory } from '@datahub/data-models/models/aspects/institutional-memory';
import { IBaseEntity } from '@datahub/metadata-types/types/entity';
import { readEntity } from '@datahub/data-models/api/entity';
import { relationship } from '@datahub/data-models/relationships/decorator';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { SocialAction } from '@datahub/data-models/constants/entity/person/social-actions';
import {
  readLikesForEntity,
  addLikeForEntity,
  removeLikeForEntity,
  readFollowsForEntity,
  addFollowForEntity,
  removeFollowForEntity
} from '@datahub/data-models/api/common/social-actions';
import { DataModelName } from '@datahub/data-models/constants/entity';
import { getDefaultIfNotFoundError } from '@datahub/utils/api/error';
import { noop } from 'lodash';
import { aspect, setAspect } from '@datahub/data-models/entity/utils/aspects';

/**
 * Options for get category method
 */
interface IGetCategoryOptions {
  // Page number to fetch from BE starting from 0
  page?: number;
  // number of items per page that the BE should return
  count?: number;
}

/**
 * Parameters for the BaseEntity static method, getLinkForEntity
 * @interface IGetLinkForEntityParams
 */
interface IGetLinkForEntityParams {
  // The display text for the generated entity link, not related to BaseEntity['displayName']
  displayName: string;
  // The URN for the specific entity instance
  entityUrn: string;
  // Optional text for the title attribute in the consuming anchor element
  title?: string;
}

/**
 * Interfaces and abstract classes define the "instance side" of a type / class,
 * therefore ts will only check the instance side
 * To check the static side / constructor, the interface should define the constructor, then static properties
 *
 * statics is a generic ClassDecorator that checks the static side T of the decorated class
 * @template T {T extends new (...args: Array<any>) => void} constrains T to constructor interfaces
 * @type {() => ClassDecorator}
 */
export const statics = <T extends new (...args: Array<unknown>) => void>(): ((c: T) => void) => noop;

/**
 * Defines the interface for the static side or constructor of a class that extends BaseEntity<T>
 * @export
 * @interface IBaseEntityStatics
 * @template T constrained by the IBaseEntity interface, the entity interface that BaseEntity subclass will encapsulate
 */
export interface IBaseEntityStatics<T, S = Snapshot> {
  new (urn: string): BaseEntity<T, S>;

  /**
   * Properties that guide the rendering of ui elements and features in the host application
   * @readonly
   * @static
   */
  renderProps: IEntityRenderProps;

  /**
   * Statically accessible name of the concrete DataModel type
   * @type {string}
   */
  displayName: string;

  /**
   * Queries the entity's endpoint to retrieve the list of nodes that are contained in the hierarchy
   * @param {(Array<string>)} args list of string values corresponding to the different hierarchical categories for the entity
   */
  readCategories(args: Array<string>, options?: IGetCategoryOptions): Promise<IBrowsePath>;

  /**
   * Queries the batch GET endpoint for snapshots for the supplied urns
   */
  readSnapshots(_urns: Array<string>): Promise<Array<S>>;

  /**
   * Builds a search query keyword from a list of segments for the related DataModelEntity
   * @memberof IBaseEntityStatics
   */
  getQueryForHierarchySegments(_segments: Array<string>): string;

  /**
   * Gets the entity link for the current entity
   */
  getLinkForEntity(params: { entityUrn: string; displayName: string }): IEntityLinkAttrs | void;
}

/**
 * Check if entity extends baseEntity by checking on the urn property
 */
export const isBaseEntity = <T extends {}>(entity?: T | IBaseEntity): entity is IBaseEntity =>
  (entity && entity.hasOwnProperty('urn')) || false;

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
export abstract class BaseEntity<T extends {} | IBaseEntity, S extends {} | Snapshot = Snapshot> {
  /**
   * A reference to the derived concrete entity instance
   * @type {T}
   */
  entity?: T;

  /**
   * TODO META-10097: We should be consistent with entity and use a generic type
   * References the Snapshot for the related Entity
   * @type {Snapshot}
   */
  snapshot?: S;

  /**
   * References the wiki related documents and objects related to this entity
   */
  _institutionalMemories?: Array<InstitutionalMemory>;

  /**
   * Getter and setter for institutional memories
   */
  @computed('_institutionalMemories')
  get institutionalMemories(): Array<InstitutionalMemory> | undefined {
    return this._institutionalMemories;
  }

  set institutionalMemories(institutionalMemories: Array<InstitutionalMemory> | undefined) {
    set(this, '_institutionalMemories', institutionalMemories);
  }

  /**
   * Hook for custom fetching operations after entity is created
   */
  onAfterCreate(): Promise<void> {
    return Promise.resolve();
  }

  /**
   * A dictionary of host Ember application routes which can be used as route arguments to the link-to helper
   */
  get hostRoutes(): Record<string, AppRoute | void> {
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
    return isBaseEntity<T>(this.entity) ? this.entity.removed : false;
  }

  /**
   * Selects the list of owners from the ownership aspect attribute of the entity
   * All entities have an ownership aspect have this property exists on the base and is inherited
   *
   * This provides the full Owner objects in a list, if what is needed is just the list of
   * owner urns, the macro value ownerUrns provides that immediately
   * @readonly
   * @type {Array<IOwner>}
   * @memberof BaseEntity
   */
  @computed('snapshot')
  get owners(): Array<Com.Linkedin.Common.Owner> {
    const ownership = getMetadataAspect(this.snapshot as Snapshot)(
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
  @map('owners.[]', ({ owner }: Com.Linkedin.Common.Owner): string => owner)
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
   * Base Ember route reference for entity pages
   * @static
   */
  static entityBaseRoute: EntityPageRoute = 'entity-type.urn';

  /**
   * Base entity display name
   * @static
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
  get staticInstance(): IBaseEntityStatics<T, S> {
    return (this.constructor as unknown) as IBaseEntityStatics<T, S>;
  }

  /**
   * Will read the current path for an entity
   */
  get readPath(): Promise<Array<string>> {
    const { urn, staticInstance } = this;
    const entityName = staticInstance.renderProps.apiEntityName;
    return readBrowsePath({
      type: entityName,
      urn
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
  get readEntity(): Promise<T> | Promise<undefined> {
    const { entityPage } = this.staticInstance.renderProps;
    if (entityPage && entityPage.apiRouteName) {
      return readEntity<T>(this.urn, entityPage.apiRouteName);
    }
    // Implemented in concrete class, if it exists for the entity
    throw new Error(NotImplementedError);
  }

  /**
   * Asynchronously resolves with the Snapshot for the entity
   * This should be implemented on the concrete class and is enforced to be available with the
   * abstract modifier
   *
   * Backend is moving away from snapshot api. UI won't enforce implementing this fn.
   * @readonly
   * @type {Promise<Snapshot>}
   */
  get readSnapshot(): Promise<Snapshot> | Promise<undefined> {
    // Implemented in concrete class, if it exists for the entity
    return Promise.resolve(undefined);
  }

  /**
   * Every entity should have a way to return the name.
   * This can be used for search results or entity header
   */
  get name(): string {
    // Implemented in concrete class
    throw new Error(NotImplementedError);
  }

  /**
   * Returns a link for the entity page for this entity
   */
  get entityLink(): IEntityLinkAttrs | void {
    return this.staticInstance.getLinkForEntity({
      entityUrn: this.urn,
      displayName: this.name
    });
  }

  /**
   * Constructs a link to a specific entity tab using the supplied tab name
   * @param {string} tabName the name of the tab to generate a link for
   */
  entityTabLink(tabName: string): this['entityLink'] {
    const { entityLink } = this;

    if (entityLink) {
      const {
        link,
        link: { model = [], route }
      } = entityLink;

      return { ...entityLink, link: { ...link, route: `${route}.tab` as AppRoute, model: [...model, tabName] } };
    }

    return entityLink;
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
   * Queries the entity's endpoint to retrieve the list of nodes that are contained in the hierarchy
   * @param {(Array<string>)} args list of string values corresponding to the different hierarchical categories for the entity
   */
  static async readCategories(
    segments: Array<string>,
    { page = 0, count = 100 }: IGetCategoryOptions = {}
  ): Promise<IBrowsePath> {
    const { browse } = this.renderProps;
    if (browse) {
      const cleanSegments: Array<string> = segments.filter(Boolean) as Array<string>;
      const defaultFacets: Record<string, Array<string>> = browse.attributes
        ? getFacetDefaultValueForEntity(browse.attributes)
        : {};
      const { elements, metadata, total } = await readBrowse({
        type: this.renderProps.apiEntityName,
        path: cleanSegments.length > 0 ? `/${cleanSegments.join('/')}` : '',
        count,
        start: page * count,
        ...defaultFacets
      });
      // List of entities
      // Create links for the entities for the current category
      const entityLinks: Array<IEntityLinkAttrs> = elements
        .map((element): IEntityLinkAttrs | void =>
          this.getLinkForEntity({
            displayName: element.name,
            entityUrn: element.urn
          })
        )
        .filter((link): boolean => Boolean(link)) as Array<IEntityLinkAttrs>; // filter removed undefined

      // List of folders
      // For this category will append and create a link for the next category (the one that you can potentially go)
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
        totalNumEntities: metadata.totalNumEntities,
        entitiesPaginationCount: total,
        entities: entityLinks,
        groups: categoryLinks
      };
    }

    // if no browse available return empty
    return {
      segments: [],
      title: '',
      totalNumEntities: 0,
      entitiesPaginationCount: 0,
      entities: [],
      groups: []
    };
  }

  /**
   * Will generate a link for an entity based on a displayName and a entityUrn
   * displayName attribute is used in the anchor tag as a the text representation if provided, is unrelated to BaseEntity['displayName']
   * optionally, a title attribute can be provided to generate the consuming anchor element title
   * @static
   * @param {IGetLinkForEntityParams} params parameters for generating the link object matching the IEntityLinkAttrs interface
   */
  static getLinkForEntity(params: IGetLinkForEntityParams): IEntityLinkAttrs | void {
    const entityPage = this.renderProps.entityPage;
    const { displayName = this.displayName, entityUrn, title = displayName } = params;

    if (entityPage && entityUrn) {
      const model = entityPage.route === 'entity-type.urn' ? [this.displayName, entityUrn] : [entityUrn];

      const link: EntityLinkNode = {
        route: entityPage.route,
        text: displayName,
        title,
        model
      };

      return {
        entity: this.displayName,
        link
      };
    }
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
    const link: EntityLinkNode<{ path: string }> = {
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
    return `browsePaths:\\/${segments.join('\\/').replace(/\s/gi, '\\ ')}`;
  }

  // TODO META-12149 this should be part of an Aspect. This fns can't live under BaseEntity as
  // then we would have a circular dependency:
  // BaseEntity -> InstitutionalMemory -> PersonEntity -> BaseEntity
  /**
   * Retrieves a list of wiki documents related to the particular entity instance
   * @readonly
   */
  readInstitutionalMemory(): Promise<Array<InstitutionalMemory>> {
    throw new Error(NotImplementedError);
  }

  // TODO META-12149 this should be part of an Aspect. This fns can't live under BaseEntity as
  // then we would have a circular dependency:
  // BaseEntity -> InstitutionalMemory -> PersonEntity -> BaseEntity
  /**
   * Writes a list of wiki documents related to a particular entity instance to the api layer
   */
  writeInstitutionalMemory(): Promise<void> {
    throw new Error(NotImplementedError);
  }

  /**
   * For social features, we flag whether or not the entity is opted into social actions
   * @default true
   */
  allowedSocialActions: Record<SocialAction, boolean> = {
    like: true,
    follow: true,
    save: true
  };

  /**
   * For this particular entity, get the list of like actions related to the entity,
   * effectively getting the number of people that have upvoted the entity
   */
  async readLikes(): Promise<void> {
    if (this.allowedSocialActions.like) {
      const likes = await readLikesForEntity(this.displayName as DataModelName, this.urn).catch(
        getDefaultIfNotFoundError({ actions: [] })
      );
      setAspect(this, 'likes', likes);
    }
  }

  /**
   * For this particular entity, add the user's like action to the entity
   */
  async addLike(): Promise<void> {
    if (this.allowedSocialActions.like) {
      const updatedLikes = await addLikeForEntity(this.displayName as DataModelName, this.urn);

      setAspect(this, 'likes', updatedLikes);
    }
  }

  /**
   * For this particular entity, remove the user's like action from the entity
   */
  async removeLike(): Promise<void> {
    if (this.allowedSocialActions.like) {
      const updatedLikes = await removeLikeForEntity(this.displayName as DataModelName, this.urn);

      setAspect(this, 'likes', updatedLikes);
    }
  }

  /**
   * For this particular entity, get the list of follow actions related to the entity,
   * effectively getting the number of people that have subscribed to notifications for metadata
   * changes in the entity
   */
  async readFollows(): Promise<void> {
    if (this.allowedSocialActions.follow) {
      const follow = await readFollowsForEntity(this.displayName as DataModelName, this.urn).catch(
        getDefaultIfNotFoundError({ followers: [] })
      );
      setAspect(this, 'follow', follow);
    }
  }

  /**
   * For this particular entity, add the user's follow action to the entity, subscribing them to
   * updates for metadata changes
   */
  async addFollow(): Promise<void> {
    if (this.allowedSocialActions.follow) {
      const follow = await addFollowForEntity(this.displayName as DataModelName, this.urn);
      setAspect(this, 'follow', follow);
    }
  }

  /**
   * For this particular entity, remove the user's follow action from the entity, unsubscribing
   * them from changes
   */
  async removeFollow(): Promise<void> {
    if (this.allowedSocialActions.follow) {
      const follow = await removeFollowForEntity(this.displayName as DataModelName, this.urn);
      setAspect(this, 'follow', follow);
    }
  }
  /**
   * Likes aspect that will reference to `entity.likes` or to the value passed to
   * setAspect('com.linkedin.common.Likes')
   */
  @aspect('com.linkedin.common.Likes')
  likes!: Com.Linkedin.Common.Likes;

  /**
   * Follow aspect that will reference to `entity.follow` or to the value passed to
   * setAspect('com.linkedin.common.Follow')
   */
  @aspect('com.linkedin.common.Follow')
  follow?: Com.Linkedin.Common.Follow;

  /**
   * EntityTopUsage aspect will reference to `entity.entityTopUsage` or to the value passed to
   * setAspect('com.linkedin.common.EntityTopUsage')
   */
  @aspect('com.linkedin.common.EntityTopUsage')
  entityTopUsage?: Com.Linkedin.Common.EntityTopUsage;

  /**
   * For social features, we add the concept of "liking" an entity which implies that the data
   * related to the entity is useful or of importance
   */
  @mapBy('likes.actions', 'likedBy')
  likedByUrns!: Array<string>;

  /**
   * For social features, we translate the urn ids of the people who have liked an entity into
   * the related PersonEntity instances
   */
  @relationship('people', 'likedByUrns')
  likedBy!: Array<PersonEntity>;

  /**
   * For social features, we add the concept of "following" an entity, which means that the person
   * (current user) has opted into notifications of updates regarding this entity's metadata
   */
  @computed('follow')
  get followedByUrns(): Array<string> {
    const { follow } = this;
    // corpUser || corpGroup is guaranteed to be a string as one of them MUST be defined, as
    // dictated by our API interface
    return (follow?.followers || []).map(
      ({ follower: { corpGroup, corpUser } }): string => (corpUser || corpGroup) as string
    );
  }

  /**
   * For social features, we translate the urn ids of the people who have followed an entity into
   * the related PersonEntity instances
   */
  @relationship('people', 'followedByUrns')
  followedBy!: Array<PersonEntity>;

  /**
   * Creates an instance of BaseEntity concrete class
   * @param {string} urn the urn for the entity being instantiated. urn is a parameter property
   * @memberof BaseEntity
   */
  constructor(readonly urn: string = '') {}
}

/**
 * Adding available aspects
 */
declare module '@datahub/data-models/entity/utils/aspects' {
  export interface IAvailableAspects {
    ['likes']?: Com.Linkedin.Common.Likes;
    ['follow']?: Com.Linkedin.Common.Follow;
    ['entityTopUsage']?: Com.Linkedin.Common.EntityTopUsage;
  }
}
