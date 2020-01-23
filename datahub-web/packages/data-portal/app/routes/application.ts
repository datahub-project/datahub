import Route from '@ember/routing/route';
import { run } from '@ember/runloop';
import ApplicationRouteMixin from 'ember-simple-auth/mixins/application-route-mixin';
import Configurator, { getConfig } from 'wherehows-web/services/configurator';
import { makeAvatar } from 'wherehows-web/constants/avatars/avatars';
import { inject as service } from '@ember/service';
import { UnWrapPromise } from 'wherehows-web/typings/generic';
import CurrentUser from '@datahub/shared/services/current-user';
import BannerService from 'wherehows-web/services/banners';
import Controller from '@ember/controller';
import { IAvatar } from 'wherehows-web/typings/app/avatars';
import { IAppConfig } from '@datahub/shared/types/configurator/configurator';
import UnifiedTracking from '@datahub/tracking/services/unified-tracking';

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

  markedRendererOverride.link = (href: string, title: string, text: string): string =>
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
  /**
   * Injected service to manage the operations related to currently logged in user
   * On app load, avatar creation and loading user attributes are handled via this service
   */
  @service('current-user')
  sessionUser: CurrentUser;

  /**
   * References the application tracking service which is used for analytics activation, setup, and management
   */
  @service('unified-tracking')
  trackingService: UnifiedTracking;

  /**
   * Banner alert service
   */
  @service
  banners: BannerService;

  @service
  configurator: Configurator;

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
    const [showStagingBanner, showLiveDataWarning, showChangeManagement, avatarEntityProps, wikiLinks] = [
      getConfig('isStagingBanner', { useDefault: true, default: false }),
      getConfig('isLiveDataWarning', { useDefault: true, default: false }),
      getConfig('showChangeManagement', { useDefault: true, default: false }),
      getConfig('userEntityProps'),
      getConfig('wikiLinks')
    ];
    const sessionUser = this.sessionUser || {};
    const { userName = '', email = '', name = '', pictureLink = '' } = sessionUser.currentUser || {};
    const avatar = makeAvatar(avatarEntityProps)({ userName, email, name, imageUrl: pictureLink });

    return {
      showStagingBanner,
      showLiveDataWarning,
      showChangeManagement,
      avatar,
      helpResources: [{ link: wikiLinks['appHelp'], label: 'DataHub Wiki' }]
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
   * @private
   */
  private async _loadCurrentUser(): Promise<void> {
    const { sessionUser } = this;

    try {
      await sessionUser.load();
    } catch {
      sessionUser.invalidateSession();
    }
  }

  /**
   * Loads the application configuration object
   * @return {Promise.<any>|void}
   * @private
   */
  private _loadConfig(): Promise<IAppConfig> {
    return this.configurator.load();
  }

  /**
   * Delegates to the tracking service methods to activate tracking adapters
   * @private
   */
  private async _setupMetricsTrackers(): Promise<void> {
    const tracking = await getConfig('tracking');
    this.trackingService.setupTrackers(tracking);
  }

  /**
   * Tracks the currently logged in user
   * @return {Promise.<isEnabled|((feature:string)=>boolean)|*>}
   * @private
   */
  private async _trackCurrentUser(): Promise<void> {
    const tracking = await getConfig('tracking');
    this.trackingService.setCurrentUser(tracking);
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
    scheduleOnce('afterRender', this, (): void =>
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
