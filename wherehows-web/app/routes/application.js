import Route from '@ember/routing/route';
import { inject as service } from '@ember/service';
import { run } from '@ember/runloop';
import { computed, set, get, setProperties, getProperties, getWithDefault, observer } from '@ember/object';
import ApplicationRouteMixin from 'ember-simple-auth/mixins/application-route-mixin';
import { feedback, avatar } from 'wherehows-web/constants';

const { mail, subject, title } = feedback;
const { url: avatarUrl } = avatar;

export default Route.extend(ApplicationRouteMixin, {
  // Injected Ember#Service for the current user
  sessionUser: service('current-user'),

  /**
   * Runtime application configuration options
   * @type {Ember.Service}
   */
  configurator: service(),

  /**
   * Metrics tracking service
   * @type {Ember.Service}
   */
  metrics: service(),

  /**
   * Attempt to load the current user and application configuration options
   * @returns {Promise}
   */
  beforeModel() {
    this._super(...arguments);

    return Promise.all([this._loadCurrentUser(), this._loadConfig()]);
  },

  /**
   * Returns an object containing properties for the application route
   * @return {{feedbackMail: {href: string, target: string, title: string}}}
   * @override
   */
  async model() {
    const isInternal = await get(this, 'configurator.getConfig')('isInternal');
    const { userName } = get(this, 'sessionUser.currentUser') || {};

    /**
     * properties for the navigation link to allow a user to provide feedback
     * @type {{href: string, target: string, title: string}}
     */
    const feedbackMail = {
      title,
      href: `mailto:${mail}?subject=${subject}`,
      target: '_blank'
    };

    const brand = {
      logo: isInternal ? '/assets/assets/images/wherehows-logo.png' : '',
      avatarUrl: isInternal ? avatarUrl.replace('[username]', userName) : '/assets/assets/images/default_avatar.png'
    };

    return { feedbackMail, brand };
  },

  /**
   * Perform post model operations
   * @return {Promise}
   */
  afterModel() {
    this._super(...arguments);

    return Promise.all([this._setupMetricsTrackers(), this._trackCurrentUser()]);
  },

  /**
   * Augments sessionAuthenticated.
   * @override ApplicationRouteMixin.sessionAuthenticated
   */
  sessionAuthenticated() {
    this._super(...arguments);
    this._loadCurrentUser();
  },

  /**
   * Internal method to invoke the currentUser service's load method
   *   If an exception occurs during the load for the current user,
   *   invalidate the session.
   * @returns {Promise<T, V>|RSVP.Promise<any>|Ember.RSVP.Promise<any, any>|Promise.<T>}
   * @private
   */
  _loadCurrentUser() {
    return get(this, 'sessionUser')
      .load()
      .catch(() => get(this, 'sessionUser').invalidateSession());
  },

  /**
   * Loads the application configuration object
   * @return {Promise.<any>|void}
   * @private
   */
  _loadConfig() {
    return get(this, 'configurator.load')();
  },

  /**
   * Requests app configuration props then if enabled, sets up metrics
   *   tracking using the supported trackers
   * @return {Promise.<TResult>}
   * @private
   */
  async _setupMetricsTrackers() {
    const { tracking = {} } = await get(this, 'configurator.getConfig')();

    if (tracking.isEnabled) {
      const metrics = get(this, 'metrics');
      const { trackers = {} } = tracking;
      const { piwikSiteId, piwikUrl = '//piwik.corp.linkedin.com/piwik/' } = trackers.piwik;

      metrics.activateAdapters([
        {
          name: 'Piwik',
          environments: ['all'],
          config: {
            piwikUrl,
            siteId: piwikSiteId
          }
        }
      ]);
    }
  },

  /**
   * Tracks the currently logged in user
   * @return {Promise.<isEnabled|((feature:string)=>boolean)|*>}
   * @private
   */
  async _trackCurrentUser() {
    const { tracking = {} } = await get(this, 'configurator.getConfig')();

    // Check if tracking is enabled prior to invoking
    // Passes an anonymous function to track the currently logged in user using the singleton `current-user` service
    return (
      tracking.isEnabled &&
      get(this, 'sessionUser').trackCurrentUser(userId => get(this, 'metrics').identify({ userId }))
    );
  },

  init() {
    this._super(...arguments);
    run.scheduleOnce('afterRender', this, 'processLegacyDomOperations');
  },

  processLegacyDomOperations() {
    // TODO: DSS-6122 Refactor Remove tree legacy operations & references
    window.legacyMain();
  }
});
