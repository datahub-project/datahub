import Component from '@ember/component';
import { inject } from '@ember/service';
import ComputedProperty from '@ember/object/computed';
import { get, set } from '@ember/object';
import { action } from 'ember-decorators/object';

import UserLookupService from 'wherehows-web/services/user-lookup';
import { OwnerIdType } from 'wherehows-web/utils/api/datasets/owners';
import { defaultOwnerProps } from 'wherehows-web/constants/datasets/owner';
import { IOwner } from 'wherehows-web/typings/api/datasets/owners';

export default class UserLookup extends Component {
  classNames = ['user-lookup'];

  /**
   * External action that receives an owner
   * @param {IOwner} user the owner instance found matching the sought user
   * @memberof UserLookup
   */
  didFindUser: (user: IOwner) => void;

  /**
   * UserLookup service to match user search string
   * @type {ComputedProperty<UserLookupService>}
   * @memberof UserLookup
   */
  userLookup: ComputedProperty<UserLookupService> = inject();
  /**
   * Reference to the userNamesResolver function to asynchronously match userNames
   * @type {UserLookupService.userNamesResolver}
   * @memberof UserLookup
   */
  userNamesResolver: UserLookupService['userNamesResolver'];

  constructor() {
    super(...arguments);

    set(this, 'userNamesResolver', get(get(this, 'userLookup'), 'userNamesResolver'));
  }

  /**
   * Async action to fetch a user matching the supplied username
   * @param {string} userName the unique username to search for
   * @returns {Promise<void>}
   * @memberof UserLookup
   */
  @action
  async findUser(userName: string): Promise<void> {
    // If a userName is not provided do nothing
    if (!userName) {
      return;
    }

    const { didFindUser } = this;
    const findUser = get(get(this, 'userLookup'), 'getPartyEntityWithUserName');
    const userEntity = await findUser(userName);

    if (userEntity) {
      const { label, displayName, category } = userEntity;
      const isGroup = category === OwnerIdType.Group.toLowerCase();
      const entity = {
        ...defaultOwnerProps,
        isGroup,
        userName: label,
        name: displayName,
        idType: isGroup ? OwnerIdType.Group : OwnerIdType.User
      };

      return void didFindUser(entity);
    }
  }
}
