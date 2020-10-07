import Component from '@ember/component';
import { tagName } from '@ember-decorators/component';
import Configurator from '@datahub/shared/services/configurator';
import { DataModelEntity } from '@datahub/data-models/constants/entity';
import { inject as service } from '@ember/service';
import CurrentUser from '@datahub/shared/services/current-user';
import { computed } from '@ember/object';
import { alias } from '@ember/object/computed';
import { IAvatar } from '@datahub/utils/types/avatars';
import { makeAvatar } from '@datahub/utils/function/avatar';
import DataModelsService from '@datahub/data-models/services/data-models';
import { AppName } from '@datahub/shared/constants/global';
import { isOwnableEntity } from '@datahub/data-models/utils/ownership';
import FoxieService from '@datahub/shared/services/foxie';

// TODO: [META-10081] Current User avatar and profile link should be tested

/**
 * Main Navigation Bar for Datahub App
 */
@tagName('')
export default class Navbar extends Component {
  /**
   * Name of the app being displayed in the Navbar.
   */
  appName: string = AppName;

  /**
   * Injected service for our virtual assistant
   */
  @service
  foxie: FoxieService;

  /**
   * Injected service for current user to determine our user profile links and the avatar image
   * properties
   */
  @service('current-user')
  sessionUser: CurrentUser;

  /**
   * Injected service for our configurator to access the user entity props
   */
  @service('configurator')
  configurator: Configurator;

  /**
   * Injected service for our data models getter to access the PersonEntity class
   */
  @service('data-models')
  dataModels!: DataModelsService;

  /**
   * The list of entities available
   */
  entities: Array<DataModelEntity> = this.dataModels.guards.unGuardedEntities.filter(isOwnableEntity);

  /**
   * Based on the current user, load an avatar properties object. Void if a currentUser is not
   * logged in or not loaded yet
   */
  @computed('sessionUser.entity')
  get avatar(): IAvatar | void {
    const { configurator, sessionUser } = this;

    if (sessionUser.entity) {
      const { username = '', email = '', name = '' } = sessionUser.entity;
      const avatarEntityProps = configurator.getConfig('userEntityProps');
      return makeAvatar(avatarEntityProps)({ userName: username, email, name });
    }
  }

  /**
   * Based on the current logged in user, create a urn. Void if a currentUser is not logged in or
   * not loaded yet
   */
  @computed('sessionUser.entity')
  get userUrn(): string | void {
    return this.sessionUser?.entity?.urn;
  }

  get showVirtualAssistant(): boolean {
    return Boolean(this.configurator.getConfig('showFoxie', { useDefault: true, default: false }));
  }

  @alias('foxie.isActive')
  isVirtualAssistantActive!: boolean;

  @alias('foxie.toggleFoxieActiveState')
  toggleVirtualAssistant!: Function;
}
