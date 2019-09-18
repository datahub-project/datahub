import getActorFromUrn from '@datahub/data-models/utils/get-actor-from-urn';
import { computed } from '@ember/object';
import { NotImplementedError } from '@datahub/data-models/constants/entity/shared';
import {
  getRenderProps,
  IPersonEntitySpecificConfigs,
  getPersonEntitySpecificRenderProps
} from '@datahub/data-models/entity/person/render-props';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { BaseEntity, statics, IBaseEntityStatics } from '@datahub/data-models/entity/base-entity';
import { IBaseEntity } from '@datahub/metadata-types/types/entity';
import { IEntityRenderProps } from '@datahub/data-models/types/entity/rendering/entity-render-props';
import { DataModelEntity } from '@datahub/data-models/constants/entity';

// TODO: [META-9699] Temporarily using IBaseEntity until we have a proposed API structure for
// IPersonEntity
@statics<IBaseEntityStatics<IBaseEntity>>()
export class PersonEntity extends BaseEntity<IBaseEntity> {
  /**
   * The human friendly alias for Dataset entities
   */
  static displayName: 'people' = 'people';

  /**
   * Static util function that can extract a username from the urn for a person entity using whatever
   * custom logic is necessary to accomplish this
   * @param urn - person entity identifier
   * @deprecated
   * Should be removed as part of open source. Definition will be on LiPersonEntity
   * TODO: [META-9698] Migrate to using LiPersonEntity
   */
  static usernameFromUrn(urn: string): string {
    return getActorFromUrn(urn);
  }

  /**
   * Static util function that can reverse the extraction of a username from urn for a person
   * entity and return to a urn (assuming the two are different)
   * IMPLEMENTATION NEEDED - This is only an open source interface definition
   * @param {string} username - the username to be converted
   * @static
   */
  static urnFromUsername: (username: string) => string;

  /**
   * Static util function that can provide a profile page link for a particular username
   * @param username - username for the person entity. Can be different from urn
   * @deprecated
   * Should be removed as part of open source. Definition will be on LiPersonEntity
   * TODO: [META-9698] Migrate to using LiPersonEntity
   */
  static profileLinkFromUsername(username: string): string {
    return `${username}`;
  }

  /**
   * Class properties common across instances
   * Dictates how visual ui components should be rendered
   * Implemented as a getter to ensure that reads are idempotent
   * @readonly
   * @static
   */
  static get renderProps(): IEntityRenderProps {
    return getRenderProps();
  }

  static ownershipEntities: Array<{ entity: DataModelEntity; getter: keyof PersonEntity }> = [
    { entity: DatasetEntity, getter: 'readDatasetOwnership' }
  ];

  /**
   * Properties for render props that are only applicable to the person entity. Dictates how UI
   * components should be rendered for this entity
   */
  static get personEntityRenderProps(): IPersonEntitySpecificConfigs {
    return getPersonEntitySpecificRenderProps();
  }

  /**
   * Combined render properties for the generic entity render props + all person entity specific
   * render properties
   */
  static get allRenderProps(): IEntityRenderProps & IPersonEntitySpecificConfigs {
    return { ...getRenderProps(), ...getPersonEntitySpecificRenderProps() };
  }

  /**
   * Allows access to the static display name of the entity from an instance
   */
  get displayName(): 'people' {
    return PersonEntity.displayName;
  }

  /**
   * The person's human readable name
   */
  name!: string;

  /**
   * The person's title at the company
   */
  title!: string;

  /**
   * Url link to the person's profile picture
   */
  profilePictureUrl!: string;

  /**
   * identifier for the person that this person reports to
   */
  reportsToUrn?: string;

  /**
   * Actual reference to related entity for this person
   */
  reportsTo?: PersonEntity;

  /**
   * User's email address
   */
  email!: string;

  /**
   * A list of skills that this particular person entity has declared to own.
   */
  skills: Array<string> = [];

  /**
   * A link to the user's linkedin profile
   */
  linkedinProfile?: string;

  /**
   * A link to the user through slack
   */
  slackLink?: string;

  /**
   * List of datasets owned by this particular user entity
   */
  datasetOwnership?: Array<DatasetEntity>;

  /**
   * User-provided focus area, describing themselves and what they do
   */
  focusArea: string = '';

  /**
   * Tags that in aggregate denote which team and organization to which the user belongs
   */
  teamTags: Array<string> = [];

  /**
   * Computes the username for easy access from the urn
   * @type {string}
   * @deprecated
   * Should be removed in favor of adding this to internal version of the class
   * TODO: [META-9698] Migrate to using LiPersonEntity
   */
  @computed('urn')
  get username(): string {
    return getActorFromUrn(this.urn);
  }

  /**
   * Computed from the username to grab the profile link for easy access
   * @type {string}
   */
  @computed('username')
  get profileLink(): string {
    return PersonEntity.profileLinkFromUsername(this.username);
  }

  /**
   * Retrieves the basic entity information for the person
   */
  get readEntity(): Promise<IBaseEntity> {
    throw new Error(NotImplementedError);
  }

  /**
   * Reads the datasets for which this person entity has ownership.
   */
  readDatasetOwnership(): Promise<Array<DatasetEntity>> {
    throw new Error(NotImplementedError);
  }
}
