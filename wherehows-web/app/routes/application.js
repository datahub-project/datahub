import Route from '@ember/routing/route';
import { inject as service } from '@ember/service';
import { run } from '@ember/runloop';
import { get } from '@ember/object';
import ApplicationRouteMixin from 'ember-simple-auth/mixins/application-route-mixin';
import { feedback, avatar } from 'wherehows-web/constants';
import Configurator from 'wherehows-web/services/configurator';
import { getAvatarProps } from 'wherehows-web/constants/avatars/avatars';

const { mail, subject, title } = feedback;

export default Route.extend(ApplicationRouteMixin, {
  // Injected Ember#Service for the current user
  sessionUser: service('current-user'),

  /**
   * Metrics tracking service
   * @type {Ember.Service}
   */
  metrics: service(),

  /**
   * Banner alert service
   * @type {Ember.Service}
   */
  banners: service(),

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
    const { getConfig } = Configurator;
    const [showStagingBanner, showLiveDataWarning, avatarEntityProps] = [
      getConfig('isStagingBanner', { useDefault: true, default: false }),
      getConfig('isLiveDataWarning', { useDefault: true, default: false }),
      getConfig('userEntityProps')
    ];
    const { userName, email, name } = get(this, 'sessionUser.currentUser') || {};
    const avatar = getAvatarProps(avatarEntityProps)({ userName, email, name });

    /**
     * properties for the navigation link to allow a user to provide feedback
     * @type {{href: string, target: string, title: string}}
     */
    const feedbackMail = {
      title,
      href: `mailto:${mail}?subject=${subject}`,
      target: '_blank'
    };

    return { feedbackMail, showStagingBanner, showLiveDataWarning, avatar };
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
    return Configurator.load();
  },

  /**
   * Requests app configuration props then if enabled, sets up metrics
   *   tracking using the supported trackers
   * @return {Promise.<T>}
   * @private
   */
  async _setupMetricsTrackers() {
    const { tracking = {} } = await Configurator.getConfig();

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
    const { tracking = {} } = await Configurator.getConfig();

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

  /**
   * At a more granular level, initializing the banner before the render loop of the entire page ends will results in the
   * render loop of the application breaking the css transition animation for our initial banners. This hook is being used
   * to schedule banners only after initial render has taken place in order to allow users see the banner animation
   * on entry
   */
  renderTemplate() {
    this._super(...arguments);
    const { showStagingBanner, showLiveDataWarning } = get(this, 'controller').get('model');
    const banners = get(this, 'banners');
    run.scheduleOnce('afterRender', this, banners.appInitialBanners.bind(banners), [
      showStagingBanner,
      showLiveDataWarning
    ]);
  },

  processLegacyDomOperations() {
    const markedRendererOverride = new marked.Renderer();

    markedRendererOverride.link = (href, title, text) =>
      `<a href='${href}' title=${title || text} target='_blank'>${text}</a>`;

    marked.setOptions({
      gfm: true,
      tables: true,
      renderer: markedRendererOverride
    });
  },

  /**
   * To make keyword accesible to the search bar
   * we need to read it from the search query parameters
   * and pass it down to the controller.
   * @param {*} controller
   * @param {*} model
   */
  setupController(controller, model) {
    const keyword = this.paramsFor('search').keyword;
    controller.set('keyword', keyword);
    controller.set('model', model);
  },

  actions: {
    /**
     * Make sure we keep the keywork updated, so if we return
     * home, the search term clears
     */
    willTransition() {
      const controller = this.controllerFor('application');
      const keyword = this.paramsFor('search').keyword;
      controller.set('keyword', keyword);
    }
  }
});
