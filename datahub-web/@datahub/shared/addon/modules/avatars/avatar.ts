import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { reads } from '@ember/object/computed';
import { INachoDropdownOption } from '@nacho-ui/core/types/nacho-dropdown';
import { IEntityLinkAttrs } from '@datahub/data-models/types/entity/shared';

/**
 * The Avatar class is meant to be used with the components that are related to rendering user
 * avatar templates. This class wraps around an underlying person entity and provides the interface
 * needed to interact with the avatar components
 */
export class Avatar {
  /**
   * Given a person entity, builds a fully functional avatar class by running the underlying
   * methods necessary for our avatar to have the data it needs to operate.
   * @param {PersonEntity} entity - entity that provides the underlying avatar information
   */
  static async build(entity: PersonEntity): Promise<Avatar> {
    await entity.readEntity;
    return new this(entity);
  }

  /**
   * The underlying person entity whose data will power an avatar instance. Marked as private as
   * the interactions with the Avatar class should be strictly related to avatars, it shouldn't
   * be some weird window through which to access the person entity.
   */
  private entity: PersonEntity;

  /**
   * Allows us to access class static properties in instances
   */
  get staticInstance(): typeof Avatar {
    return this.constructor as typeof Avatar;
  }

  /**
   * Equivalent side effects to calling the .build() method on the static class, but this is used
   * on the instance when we already have an un-built instance and want to run build without having
   * to create a new class.
   */
  async build(): Promise<void> {
    await this.entity.readEntity;
  }

  /**
   * Username for the person whose avatar this represents
   */
  @reads('entity.username')
  username!: PersonEntity['username'];

  /**
   * Urn for the person whose avatar this represents
   */
  @reads('entity.urn')
  urn!: PersonEntity['urn'];

  /**
   * Email for the person whose avatar this represents
   */
  @reads('entity.email')
  email!: PersonEntity['email'];

  /**
   * Name for the person whose avatar this represents
   */
  @reads('entity.name')
  name!: PersonEntity['name'];

  /**
   * The profile entity page link for the person whose avatar this represents
   */
  get profileLink(): IEntityLinkAttrs['link'] | void {
    // It seems we can't use optionalObject?.param when type can be void so we have to make a
    // strict check for TS not to complain
    if (this.entity.entityLink) {
      return this.entity.entityLink.link;
    }
  }

  /**
   * Selection options for an avatar with dropdown
   */
  avatarOptions?: Array<INachoDropdownOption<unknown>>;

  constructor(entity: PersonEntity) {
    this.entity = entity;
  }
}
