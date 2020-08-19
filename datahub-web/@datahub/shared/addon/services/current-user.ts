import Service from '@ember/service';
import { get, set } from '@ember/object';
import { currentUser } from '@datahub/shared/api/user/authentication';
import Session from 'ember-simple-auth/services/session';
import { inject as service } from '@ember/service';
import DataModelsService from '@datahub/data-models/services/data-models';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { ICorpUserInfo } from '@datahub/metadata-types/types/entity/person/person-entity';

/**
 * Indicates that the current user has already been tracked in the current session
 * @type {boolean}
 * @private
 */
let _hasUserBeenTracked = false;

/**
 * The current user service can be injected into our various datahub addons to give reference
 * whenever necessary to the current logged in user
 */
export default class CurrentUser extends Service {
  /**
   * Reference to the application session service, implemented with Ember Simple Auth
   * @type {ComputedProperty<Session>}
   */
  @service
  session!: Session;

  /**
   * Reference to the data models service in order to access the proper entity for our current user
   */
  @service
  dataModels!: DataModelsService;

  /**
   * Creates a PersonEntity out of the current user so that we can fetch and store information
   * about the logged in person
   */
  entity?: PersonEntity;

  /**
   * Attempt to load the currently logged in user.
   *   If the userName is found from a previously retained session,
   *   append to service. Request the full user object, and append
   *   to service.
   * @returns {Promise}
   */
  async load(): Promise<void> {
    const { session, dataModels } = this;
    // If we have a valid session, get the currently logged in user, and set the currentUser attribute,
    // otherwise raise an exception
    if (session.isAuthenticated) {
      const userV2: ICorpUserInfo = await currentUser();
      // TODO: Current midtier is falling behind, requiring us to make adjustments in open source to accommodate
      // Since we actually are calling v1 endpoint here, ICorpUserInfo is not appropriate typing. This temp workaround
      // will let us have parity with current OS while we make the fix to push out from internal
      const tempFixUserInfo = (userV2 as unknown) as { user: { userName: string } };

      const PersonEntityClass = dataModels.getModel(PersonEntity.displayName);
      const urn = PersonEntityClass.urnFromUsername(tempFixUserInfo.user.userName);
      const entity = await dataModels.createInstance(PersonEntityClass.displayName, urn);

      set(this, 'entity', entity);
    }
  }

  /**
   * Invalidates the current session if the session is currently valid
   * useful if, for example, the server is no able to provide the currently logged in user
   * @return {Promise<void>}
   */
  invalidateSession(): Promise<void> {
    const session = get(this, 'session');

    return session.isAuthenticated ? session.invalidate() : Promise.resolve();
  }

  /**
   * Uses the provided tracking function to track the currently logged in user's userId

   * This is not a self-contained method, since it depends on other values that cannot be enforced as dependency
   * of this service as a whole. The dependency on the tracking implementation is also not a concern of this service,
   * injecting as a function is the better approach.
   * @param {Function} userIdTracker a function that takes the userId and tracks it
   */
  trackCurrentUser(userIdTracker: (...args: Array<unknown>) => void = (): void => void 0): void {
    const userId: string = this.entity?.username || '';

    // If we have a non-empty userId, the user hasn't already been tracked and the userIdTracker is a valid argument
    // then track the user and toggle the flag affirmative
    if (userId && !_hasUserBeenTracked && typeof userIdTracker === 'function') {
      userIdTracker(userId);
      _hasUserBeenTracked = true;
    }
  }
}

// DO NOT DELETE: this is how TypeScript knows how to look up your services.
declare module '@ember/service' {
  // eslint-disable-next-line
  interface Registry {
    'current-user': CurrentUser;
  }
}
