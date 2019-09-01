import Route from '@ember/routing/route';
import { run } from '@ember/runloop';
import ApplicationRouteMixin from 'ember-simple-auth/mixins/application-route-mixin';
import Configurator from 'wherehows-web/services/configurator';
import { makeAvatar } from 'wherehows-web/constants/avatars/avatars';
import { inject as service } from '@ember/service';
import { UnWrapPromise } from 'wherehows-web/typings/generic';
import CurrentUser from '@datahub/shared/services/current-user';
import Metrics from 'ember-metrics';
import BannerService from 'wherehows-web/services/banners';
import Controller from '@ember/controller';
import { IAvatar } from 'wherehows-web/typings/app/avatars';
import { IAppConfig } from 'wherehows-web/typings/api/configurator/configurator';

/**
 * Quick alias of the type of the return of the model hook
 */
type Model = UnWrapPromise<ReturnType<ApplicationRoute['model']>>;

const { scheduleOnce } = run;

/**
 * Will initialize Marked which is a global (and probably from legacy code) library.
 * @param marked Marked Library
 */
const initMarked = (marked: Window['marked']): void => {
  const markedRendererOverride = new marked.Renderer();

  markedRendererOverride.link = (href: string, title: string, text: string) =>
    `<a href='${href}' title='${title || text}' target='_blank'>${text}</a>`;

  marked.setOptions({
    gfm: true,
    tables: true,
    renderer: markedRendererOverride
  });
};

interface IApplicationRouteModel {
  helpResources: Array<{ label: string; link: string }>;
  showStagingBanner: boolean;
  showLiveDataWarning: boolean;
  showChangeManagement: boolean;
  avatar: IAvatar;
}

export default class ApplicationRoute extends Route.extend(ApplicationRouteMixin) {
  // Injected Ember#Service for the current user
  @service('current-user')
  sessionUser: CurrentUser;

  /**
   * Metrics tracking service
   * @type {Metrics}
   */
  @service
  metrics: Metrics;

  /**
   * Banner alert service
   * @type {Ember.Service}
   */
  @service
  banners: BannerService;

  /**
   * Attempt to load the current user and application configuration options
   * @returns {Promise}
   */
  beforeModel(): Promise<[void, IAppConfig]> {
    super.beforeModel.apply(this, arguments);

    return Promise.all([this._loadCurrentUser(), this._loadConfig()]);
  }

  /**
   * Returns an object containing properties for the application route
   * @return {{feedbackMail: {href: string, target: string, title: string}}}
   * @override
   */
  model(): IApplicationRouteModel {
    const { getConfig } = Configurator;
    const [showStagingBanner, showLiveDataWarning, showChangeManagement, avatarEntityProps] = [
      getConfig('isStagingBanner', { useDefault: true, default: false }),
      getConfig('isLiveDataWarning', { useDefault: true, default: false }),
      getConfig('showChangeManagement', { useDefault: true, default: false }),
      getConfig('userEntityProps')
    ];
    const sessionUser = this.sessionUser || {};
    const { userName = '', email = '', name = '' } = sessionUser.currentUser || {};
    const avatar = makeAvatar(avatarEntityProps)({ userName, email, name });

    return {
      showStagingBanner,
      showLiveDataWarning,
      showChangeManagement,
      avatar,
      helpResources: []
    };
  }

  /**
   * Perform post model operations
   * @return {Promise}
   */
  afterModel(): Promise<[void, false | void]> {
    super.afterModel.apply(this, arguments);

    return Promise.all([this._setupMetricsTrackers(), this._trackCurrentUser()]);
  }

  /**
   * Augments sessionAuthenticated.
   * @override ApplicationRouteMixin.sessionAuthenticated
   */
  async sessionAuthenticated(): Promise<void> {
    // @ts-ignore waiting for this be solved: https://github.com/simplabs/ember-simple-auth/issues/1619
    super.sessionAuthenticated.apply(this, arguments);
    await this._loadCurrentUser();
  }

  /**
   * Internal method to invoke the currentUser service's load method
   *   If an exception occurs during the load for the current user,
   *   invalidate the session.
   * @returns {Promise<T, V>|RSVP.Promise<any>|Ember.RSVP.Promise<any, any>|Promise.<T>}
   * @private
   */
  _loadCurrentUser(): Promise<void> {
    const { sessionUser } = this;
    return sessionUser.load().catch(() => sessionUser.invalidateSession());
  }

  /**
   * Loads the application configuration object
   * @return {Promise.<any>|void}
   * @private
   */
  _loadConfig(): Promise<IAppConfig> {
    return Configurator.load();
  }

  /**
   * Requests app configuration props then if enabled, sets up metrics
   *   tracking using the supported trackers
   * @return {Promise.<T>}
   * @private
   */
  async _setupMetricsTrackers(): Promise<void> {
    const tracking = await Configurator.getConfig('tracking');

    if (tracking.isEnabled) {
      const metrics = this.metrics;
      const { trackers } = tracking;
      const { piwikSiteId, piwikUrl } = trackers.piwik;

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
  }

  /**
   * Tracks the currently logged in user
   * @return {Promise.<isEnabled|((feature:string)=>boolean)|*>}
   * @private
   */
  async _trackCurrentUser(): Promise<void> {
    const tracking = await Configurator.getConfig('tracking');
    const { sessionUser, metrics } = this;

    // Check if tracking is enabled prior to invoking
    // Passes an anonymous function to track the currently logged in user using the singleton `current-user` service
    tracking.isEnabled && sessionUser.trackCurrentUser((userId: string) => metrics.identify({ userId }));
  }

  init(): void {
    super.init.apply(this, arguments);
    scheduleOnce('afterRender', this, 'processLegacyDomOperations');
  }

  /**
   * At a more granular level, initializing the banner before the render loop of the entire page ends will results in the
   * render loop of the application breaking the css transition animation for our initial banners. This hook is being used
   * to schedule banners only after initial render has taken place in order to allow users see the banner animation
   * on entry
   */
  renderTemplate(_: Controller, model: Model): void {
    super.renderTemplate.apply(this, arguments);
    const { showStagingBanner, showLiveDataWarning, showChangeManagement } = model;
    const { banners } = this;
    scheduleOnce('afterRender', this, () =>
      banners.appInitialBanners([showStagingBanner, showLiveDataWarning, showChangeManagement])
    );
  }

  processLegacyDomOperations(): void {
    // As marked is a legacy global variable, lets ignore the type
    if (window.marked) {
      initMarked(window.marked);
    }
  }
}
