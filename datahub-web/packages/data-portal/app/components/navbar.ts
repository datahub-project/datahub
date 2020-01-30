import Component from '@ember/component';
import { tagName } from '@ember-decorators/component';
import Configurator from 'wherehows-web/services/configurator';
import { DataModelEntity } from '@datahub/data-models/constants/entity';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { inject as service } from '@ember/service';
import CurrentUser from '@datahub/shared/services/current-user';
import { computed } from '@ember/object';
import { IAvatar } from '@datahub/utils/types/avatars';
import { makeAvatar } from '@datahub/utils/function/avatar';
import DataModelsService from '@datahub/data-models/services/data-models';

// TODO: [META-10081] Current User avatar and profile link should be tested

/**
 * Main Navigation Bar for Datahub App
 */
@tagName('')
export default class Navbar extends Component {
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
  entities: Array<DataModelEntity> = this.dataModels.guards.unGuardedEntities.filter(
    // Note: "People I Own" should never appear on the navbar regardless of context
    (entity): boolean => entity.displayName !== PersonEntity.displayName
  );

  /**
   * Based on the current user, load an avatar properties object. Void if a currentUser is not
   * logged in or not loaded yet
   */
  @computed('sessionUser.currentUser')
  get avatar(): IAvatar | void {
    const { configurator, sessionUser } = this;

    if (sessionUser.currentUser) {
      const { userName = '', email = '', name = '' } = sessionUser.currentUser;
      const avatarEntityProps = configurator.getConfig('userEntityProps');
      return makeAvatar(avatarEntityProps)({ userName, email, name });
    }
  }

  /**
   * Based on the current logged in user, create a urn. Void if a currentUser is not logged in or
   * not loaded yet
   */
  @computed('sessionUser.currentUser')
  get userUrn(): string | void {
    const { dataModels, sessionUser } = this;
    const PersonEntityClass = dataModels.getModel('people') as typeof PersonEntity;

    if (sessionUser.currentUser) {
      return PersonEntityClass.urnFromUsername(sessionUser.currentUser.userName);
    }
  }
}
