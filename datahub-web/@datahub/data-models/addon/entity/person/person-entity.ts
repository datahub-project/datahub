import getActorFromUrn from '@datahub/data-models/utils/get-actor-from-urn';
import { computed } from '@ember/object';
import { profileLinkBase } from '@datahub/data-models/constants/entity/person/links';

export class PersonEntity {
  /**
   * Static util function that can extract a username from the urn for a person entity using whatever
   * custom logic is necessary to accomplish this
   * @param urn - person entity identifier
   */
  static usernameFromUrn(urn: string): string {
    return getActorFromUrn(urn);
  }

  /**
   * Static util function that can provide a profile page link for a particular username
   * @param username - username for the person entity. Can be different from urn
   */
  static profileLinkFromUsername(username: string): string {
    return `${profileLinkBase}${username}`;
  }

  /**
   * Identifier for the person entity
   */
  urn: string;

  /**
   * Computes the username for easy access from the urn
   * @type {string}
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

  constructor(urn: string) {
    this.urn = urn;
  }
}
