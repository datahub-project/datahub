import getActorFromUrn from '@datahub/data-models/utils/get-actor-from-urn';
import { computed, set } from '@ember/object';
import { profileLinkBase } from '@datahub/data-models/constants/entity/person/links';
import { NotImplementedError } from '@datahub/data-models/constants/entity/shared';
import {
  getRenderProps,
  IPersonEntitySpecificConfigs,
  getPersonEntitySpecificRenderProps
} from '@datahub/data-models/entity/person/render-props';
import { AclAccess } from '@datahub/data-models/entity/person/modules/acl';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { BaseEntity, statics, IBaseEntityStatics } from '@datahub/data-models/entity/base-entity';
import { IEntityRenderProps } from '@datahub/data-models/types/entity/rendering/entity-render-props';
import { DataModelEntity } from '@datahub/data-models/constants/entity';
import { IPersonEntityEditableProperties } from '@datahub/data-models/types/entity/person/props';
import { ICorpUserInfo } from '@datahub/metadata-types/types/entity/person/person-entity';
import { readPerson, saveEditablePersonalInfo } from '@datahub/data-models/api/person/entity';
import { alias, not } from '@ember/object/computed';
import { relationship } from '@datahub/data-models/relationships/decorator';
import { ListEntity } from '@datahub/data-models/entity/list/list-entity';
import { SocialAction } from '@datahub/data-models/constants/entity/person/social-actions';
import { corpUserUrnBasePrefix } from '@datahub/data-models/config/urn/base-prefix';

@statics<IBaseEntityStatics<ICorpUserInfo>>()
export class PersonEntity extends BaseEntity<ICorpUserInfo> {
  /**
   * The human friendly alias for Dataset entities
   */
  static displayName: 'people' = 'people';

  /**
   * Base url for fetching the user profile picture
   */
  static aviUrlPrimary: string;

  /**
   * Fallback url if the aviUrlPrimary url did not fetch a picture from the requested resource
   */
  static aviUrlFallback: string;

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
  static urnFromUsername(username: string): string {
    // TODO: [META-11362] Temporarily identity function until we have urn class
    return `${corpUserUrnBasePrefix}${username}`;
  }

  /**
   * Static util function that can provide a profile page link for a particular username
   * @param username - username for the person entity. Can be different from urn
   * @deprecated
   * Should be removed as part of open source. Definition will be on LiPersonEntity
   * TODO: [META-9698] Migrate to using LiPersonEntity
   */
  static profileLinkFromUsername(username: string): string {
    return `${profileLinkBase}${username}`;
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
   * Manually saved fullname, useful for when we have the name available but don't want to waste a
   * full api call on readEntity
   */
  fullName = '';

  /**
   * The person's display name
   * Try to show the name, fallback to username if not available
   */
  @computed('entity.info.fullName', 'fullName', 'username')
  get name(): string {
    const { entity } = this;
    return entity?.info?.fullName || this.fullName || this.username;
  }

  set name(value: string) {
    set(this, 'fullName', value);
  }

  /**
   * The person's title at the company
   */
  @alias('entity.info.title')
  title!: string;

  /**
   * Url link to the person's profile picture
   */
  get profilePictureUrl(): string {
    const fallbackImgUrl = PersonEntity.aviUrlFallback || '/assets/images/default_avatar.png';
    const baseUrl = PersonEntity.aviUrlPrimary;

    return baseUrl ? baseUrl.replace('[username]', (): string => this.username) : fallbackImgUrl;
  }

  /**
   * identifier for the person that this person reports to
   */
  @alias('entity.info.managerUrn')
  reportsToUrn?: string;

  /**
   * Defines the relationship for the person that this person reports to
   */
  @relationship('people', 'reportsToUrn')
  manager?: PersonEntity;

  /**
   * Actual reference to related entity for this person
   */
  @computed('entity')
  get reportsTo(): PersonEntity | void {
    const { entity, manager } = this;
    if (entity && manager) {
      const managerEntity = manager;
      set(managerEntity, 'name', entity.info.managerName || '');
      return managerEntity;
    }
  }

  /**
   * User's email address
   */
  @alias('entity.info.email')
  email!: string;

  /**
   * A list of skills that this particular person entity has declared to own.
   */
  @computed('entity.editableInfo.skills')
  get skills(): Array<string> {
    const { entity } = this;
    return (entity && entity.editableInfo && entity.editableInfo.skills) || [];
  }

  /**
   * A link to the user's linkedin profile
   */
  @computed()
  get linkedinProfile(): string | void {
    throw new Error(NotImplementedError);
  }

  /**
   * A link to the user through slack
   */
  slackLink?: string;

  /**
   * The datasets for which the specified PersonEntity has access to the underlying data
   */
  datasetsWithAclAccess?: Array<AclAccess<DatasetEntity>>;

  /**
   * List of datasets owned by this particular user entity
   */
  datasetOwnership?: Array<DatasetEntity>;

  /**
   * Alias for when to show inactive tag
   */
  @not('entity.info.active')
  inactive!: boolean;

  /**
   * User-provided focus area, describing themselves and what they do
   */
  @computed('entity.editableInfo.aboutMe')
  get focusArea(): string {
    const { entity } = this;
    return (entity && entity.editableInfo && entity.editableInfo.aboutMe) || '';
  }

  /**
   * Tags that in aggregate denote which team and organization to which the user belongs
   */
  @computed('entity.editableInfo.teams')
  get teamTags(): Array<string> {
    const { entity } = this;
    return (entity && entity.editableInfo && entity.editableInfo.teams) || [];
  }

  /**
   * Computes the username for easy access from the urn
   * @type {string}
   * @deprecated
   * Should be removed in favor of adding this to internal version of the class
   * TODO: [META-9698] Migrate to using LiPersonEntity
   */
  @computed('urn', 'entity')
  get username(): string {
    const { entity } = this;
    return entity ? entity.username : getActorFromUrn(this.urn);
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
  get readEntity(): Promise<ICorpUserInfo> {
    return readPerson(this.urn).then(
      (person): ICorpUserInfo => {
        const personApiView = {
          ...person,
          urn: this.urn
        };

        return personApiView;
      }
    );
  }

  /**
   * Prevents implementation error by overriding base entity snapshot and returning undefined
   */
  get readSnapshot(): Promise<undefined> {
    return Promise.resolve(undefined);
  }

  /**
   * Function version of get readEntity() and sets the value of personApiView to this instance
   * Useful for avoiding exposure of API concerns to the individual package levels
   */
  async retrieveAndSetEntityData(): Promise<PersonEntity> {
    const entityData = await this.readEntity;
    set(this, 'entity', entityData);
    return this;
  }

  /**
   * Reads the datasets for which this person entity has ownership.
   */
  readDatasetOwnership(): Promise<Array<DatasetEntity>> {
    throw new Error(NotImplementedError);
  }

  /**
   * Updates the editable properties for this person entity instance
   * @param {IPersonEntityEditableProperties} props - snapshot of the newly updated properties that
   *  that we want to persist
   */
  updateEditableProperties(props: IPersonEntityEditableProperties): Promise<void> {
    return saveEditablePersonalInfo(this.urn, {
      teams: props.teamTags,
      aboutMe: props.focusArea,
      skills: props.skills
    });
  }

  /**
   * The urns of the direct reports of this person, if they have any
   *
   * Note: Defined as a getter as the expectation is this property will be dependent on some
   * other base proeprty on this class
   */
  get directReportUrns(): Array<string> {
    // Note: The is intentionally left as a blank array, implementation pending
    return [];
  }

  /**
   * This person's direct reports, expressed as other instances of person entity
   */
  @relationship('people', 'directReportUrns')
  directReports!: Array<PersonEntity>;

  /**
   * The urns of the peers (same team, same level) as the person, if they have any
   *
   * Note: Defined as a getter as the expectation is this property will be dependent on some
   * other base proeprty on this class*
   */
  get peersUrns(): Array<string> {
    // Note: The is intentionally left as a blank array, implementation pending
    return [];
  }

  /**
   * This person's peers, expressed as other instances of person entity
   */
  @relationship('people', 'peersUrns')
  peers!: Array<PersonEntity>;

  /**
   * Opts out any PersonEntity from BEING liked, followed, or saved to a list
   */
  allowedSocialActions = {
    [SocialAction.LIKE]: false,
    [SocialAction.FOLLOW]: false,
    [SocialAction.SAVE]: false
  };

  /**
   * Reference by id to the list of lists that this person has publisher access to
   * Note: Refer to IMPORTANT WORDAGE in ListEntity for definition of publisher
   */
  publishedListUrns: Array<string> = [];

  /**
   * This person's published lists, expressed as the list entity instances
   */
  @relationship('lists', 'publishedListUrns')
  publishedLists!: Array<ListEntity>;

  /**
   * Urns for the entities that this person has liked
   */
  likedEntityUrns: Array<string> = [];

  /**
   * Urns for the entities that this person has followed
   */
  followedEntityUrns: Array<string> = [];

  /**
   * Placeholder function for the method that will trigger a follow function for an entity
   * @param _entityUrn - urn for the entity that we want to follow
   */
  followEntity(_entityUrn: string): void {}

  /**
   * Placeholder function for the method that will trigger a like function for an entity
   * @param _entityUrn - urn for the entity that we want to like
   */
  likeEntity(_entityUrn: string): void {}
}
