import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/avatar/containers/avatar-main';
import { inject as service } from '@ember/service';
import { set } from '@ember/object';
import { task } from 'ember-concurrency';
import { tagName, layout } from '@ember-decorators/component';
import { ETask } from '@datahub/utils/types/concurrency';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import DataModelsService from '@datahub/data-models/services/data-models';
import { Avatar } from '@datahub/shared/modules/avatars/avatar';
import { oneOrMany } from '@datahub/utils/array/one-or-many';
import { containerDataSource } from '@datahub/utils/api/data-source';

/**
 * The AvatarMainContainer is responsible for receiving information about a person and translating
 * it into an Avatar class object that can be passed into various components meant for rendering
 * avatars consistently. Because the data we receive in our application can vary, the goal of this
 * container will be to take inconsistent input and return consistent output.
 *
 * Possible parameters for this container are: entity, urn, username.
 * Input can be in the form of a single item or an array of items. The container only intends for
 * one kind of input given, if multiple inputs are provided then the priority will be:
 * entity > urn > username
 *
 * @example
 * {{!-- Basic use --}}
 * <Avatar::Containers::AvatarMain @urn={{arrayOfStringUrns}} as |avatars|>
 *   {{#each avatars as |avatar|}}
 *     <Avatar::AvatarName @avatar={{avatar}} />
 *   {{/each}}
 * </Avatar::Containers::AvatarMain>
 *
 * {{!-- Would fetch data for the avatar object as we build it --}}
 * <Avatar::Containers::AvatarMain @entity={{personEntity}} @shouldBuildAvatar={{true}} as |avatars|>
 *   ...
 * </Avatar::Containers::AvatarMain>
 */
@tagName('')
@layout(template)
@containerDataSource<AvatarMainContainer>('getAvatarsTask', ['entity', 'urn', 'username'])
export default class AvatarMainContainer extends Component {
  /**
   * Injection of data models service to access the generic PersonEntity class
   */
  @service
  dataModels!: DataModelsService;

  /**
   * Optional argument for a person entity, or a list of person entities, to underly the data
   * inside the avatar object
   */
  entity?: PersonEntity | Array<PersonEntity>;

  /**
   * Optional argument for a person entity's urn, or list of urns, to reference the entity that
   * should underly the data inside the avatar object
   */
  urn?: string | Array<string>;

  /**
   * Optional argument for a person entity's username, or list of usernames, provided when their
   * urn or entity (the safer options) are not easily accessible
   */
  username?: string | Array<string>;

  /**
   * Whether or not we need to await the async operation of building the avatars. Setting this to
   * true will await the build() method on our avatars, allowing us to access additional underlying
   * data but will cause a delay in returning the information. This is useful when we need the
   * entirety of the Avatar information *before* showing anything on the UI. However, in most cases
   * we will want to show some UI initially and load additional data as needed, so this is default
   * to false
   * @default false
   */
  shouldBuildAvatar?: boolean;

  /**
   * The avatar objects finally created from the avatar creation task, yielded from this container
   * to whatever component needs this data for rendering
   */
  avatars?: Array<Avatar>;

  /**
   * Given a urn or username argument to the container, returns as a list of urns
   */
  get urns(): Array<string> {
    const { username, urn, dataModels } = this;
    const personEntityClass = dataModels.getModel(PersonEntity.displayName);

    if (!username && !urn) {
      return [];
    }

    return urn
      ? oneOrMany(urn)
      : oneOrMany(username).map((value: string): string => personEntityClass.urnFromUsername(value));
  }

  /**
   * Regardless of the argument given to the container, this will return a list of entities that
   * should underly the avatar objects we create
   */
  get entities(): Array<PersonEntity> {
    const { entity, dataModels } = this;
    const personEntityClass = dataModels.getModel(PersonEntity.displayName);

    return entity ? oneOrMany(entity) : this.urns.map((urn): PersonEntity => new personEntityClass(urn));
  }

  /**
   * Sets the avatar information to be yielded by the container
   */
  @task(function*(this: AvatarMainContainer): IterableIterator<Promise<Array<Avatar>>> {
    const avatars: Array<Avatar> = Boolean(this.shouldBuildAvatar)
      ? (((yield Promise.all(
          this.entities.map((entity): Promise<Avatar> => Avatar.build(entity))
        )) as unknown) as Array<Avatar>)
      : this.entities.map((entity): Avatar => new Avatar(entity));

    set(this, 'avatars', avatars);
  })
  getAvatarsTask!: ETask<void>;
}
