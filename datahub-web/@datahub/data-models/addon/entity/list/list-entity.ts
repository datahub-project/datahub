import { BaseEntity, statics, IBaseEntityStatics } from '@datahub/data-models/entity/base-entity';
import { DataModelEntity } from '@datahub/data-models/constants/entity';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { relationship } from '@datahub/data-models/relationships/decorator';
import { IEntityRenderProps } from '@datahub/data-models/types/entity/rendering/entity-render-props';

/**
 * A ListEntity is an arbitrary list that a user has created to save a set of other types of
 * entities and form a logical grouping for those entities.
 *
 * IMPORTANT WORDAGE:
 * publisher - The publisher of a list is a person with edit permissions for that list, allowing them
 *  to change the metadata about the list or its contents. The user who created a list is
 *  automatically the sole publisher of that list.
 *
 * subscriber - A subscriber is someone who does not have publisher permissions on a list but still
 *  wishes to add it to their own namespace. A subscriber cannot edit the list, but will still be able
 *  to reference it from their own user profile page. This list is the same urn as the original list,
 *  meaning it is considered the same list and any updates by a publisher will be reflected in the
 *  subscribed list.
 */
@statics<IBaseEntityStatics<{}>>()
export class ListEntity extends BaseEntity<{}> {
  /**
   * Identifier for the entity
   */
  static displayName: 'lists' = 'lists';

  /**
   * Default renderProps for a list entity. We will not be dealing with list entity pages
   */
  static get renderProps(): IEntityRenderProps {
    return {
      apiEntityName: 'N/A',
      search: {
        placeholder: '',
        attributes: [],
        isEnabled: false,
        defaultAspects: []
      }
    };
  }

  /**
   * Returns the static displayName identifier attached to the class
   */
  get displayName(): 'lists' {
    return this.staticInstance.displayName as 'lists';
  }

  /**
   * A name of the list given by the user upon creation of this list
   */
  name = '';

  /**
   * A short description about the contents or purpose of the list given by the publisher
   */
  description?: string;

  /**
   * The urn identifiers for the entities that are included in this list
   */
  entityUrns: Array<string> = [];

  /**
   * The entities that are included in this list expressed as an array of instances of those entities
   */
  entities?: Array<DataModelEntity>;

  /**
   * The urn identifiers for the people that are "publishers" of this list. Definition for a publisher
   * can be found in the class documentation
   */
  publisherUrns: Array<string> = [];

  /**
   * The publishers of the list expressed as instances of PersonEntity. Definition for a publisher
   * can be found in the class documentation
   */
  @relationship('people', 'publisherUrns')
  publishers!: Array<PersonEntity>;

  /**
   * The subscribers of a list expressed as instances of PersonEntity. Definition of a subscriber
   * can be found in the class documentation
   *
   * Note: This feature is currently on the roadmap but not implemented yet so we're keeping here
   * for definition only
   */
  subscribers?: Array<PersonEntity>;
}
