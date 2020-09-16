import { inject as service } from '@ember/service';
import CurrentUser from '@datahub/shared/services/current-user';
import { alias } from '@ember/object/computed';
import { WithControllerProtocol } from '@datahub/utils/controllers/protocol';
import { JitAclProtocol } from '@datahub/shared/controllers/protocols/jit-acl';

export default class EntityTab extends WithControllerProtocol(JitAclProtocol) {
  /**
   * References the CurrentUser service
   */
  @service('current-user')
  currentUser!: CurrentUser;

  /**
   * Aliases the ldap username of the currently logged in user
   */
  @alias('currentUser.entity.username')
  userName!: string;
}
