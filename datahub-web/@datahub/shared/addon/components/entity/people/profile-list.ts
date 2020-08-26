import Component from '@glimmer/component';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { IEntityListProfile } from '@datahub/shared/types/profile-list';

interface IPeopleEntityProfileListArgs {
  /**
   * The list of people entity profiles rendered in this profile
   */
  profiles: Array<IEntityListProfile<PersonEntity>>;
}

export const baseClass = 'people-entity-profile-list';

/**
 * This component is a list of profiles for people entities.
 * Each profile will contain the person entity's picture, name, title, and connection info.
 * The profile also allows for the user to be redirected to the selected user's entity page
 *
 * TODO META-11582: Refactor people profile list to adopt Avatars architecture
 */
export default class PeopleEntityProfileList extends Component<IPeopleEntityProfileListArgs> {
  // Declared for convenient access in the template
  baseClass = baseClass;
}
