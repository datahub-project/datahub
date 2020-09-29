import Controller from '@ember/controller';
import Session from 'ember-simple-auth/services/session';
import { inject as service } from '@ember/service';
import { computed } from '@ember/object';

export default class Application extends Controller {
  /**
   * User session management service
   * Retain Session here, as it is accessed from application template
   * @type {Session}
   */
  @service
  session: Session;

  /**
   * Will determine whether of not show search hero component
   */
  @computed('currentRouteName')
  get showHero(): boolean {
    const routesWithHero = ['browse.entity.index', 'browse.index'];
    const target = this.target as { currentRouteName: string };
    return routesWithHero.includes(target.currentRouteName || '');
  }
}
