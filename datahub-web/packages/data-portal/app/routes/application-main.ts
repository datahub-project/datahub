import { run } from '@ember/runloop';
import { feedback } from 'datahub-web/constants';
import { getConfig } from '@datahub/shared/services/configurator';
import { inject as service } from '@ember/service';
import { UnWrapPromise } from '@datahub/utils/types/async';
import BannerService from '@datahub/shared/services/banners';
import Controller from '@ember/controller';
import UnifiedTracking from '@datahub/shared/services/unified-tracking';
import DataModelsService from '@datahub/data-models/services/data-models';
import { IAppConfig } from '@datahub/shared/types/configurator/configurator';
import { NotificationEvent } from '@datahub/utils/constants/notifications';
import { IMailHeaderRecord } from '@datahub/utils/helpers/email';
import AvatarService from '@datahub/shared/services/avatar';
import ApplicationBaseRoute from '@datahub/shared/routes/application-base';

/**
 * Quick alias of the type of the return of the model hook
 */
type Model = UnWrapPromise<ReturnType<ApplicationBaseRoute['model']>>;

const { subject, title } = feedback;
const { scheduleOnce } = run;

interface IApplicationRouteModel {
  // Email attributes a user to contact support to provide feedback
  feedbackMail: IMailHeaderRecord & { title: string };
  // Configuration for additional help links the user can access
  helpResources: Array<{ label: string; link: string }>;
  // Flag for showing whether or not we are in a staging environment
  showStagingBanner: boolean;
  // Flag for warning a user that they are in a testing environment with access to production data
  showLiveDataWarning: boolean;
  // Flag for showing a banner that the app is undergoing some kind of maintenance
  showChangeManagement: boolean;
  // Configurations for if we have a custom banner to display
  customBanner: IAppConfig['customBanner'];
}

export default class ApplicationMainRoute extends ApplicationBaseRoute {
  /**
   * References the application tracking service which is used for analytics activation, setup, and management
   */
  @service('unified-tracking')
  trackingService: UnifiedTracking;

  /**
   * Injected service to access the data models related to our various entities.
   */
  @service('data-models')
  dataModels: DataModelsService;

  /**
   * Injected service to load the avatars with necessary config information
   */
  @service
  avatar: AvatarService;

  /**
   * Banner alert service
   */
  @service
  banners: BannerService;

  /**
   * Returns an object containing properties for the application route
   * @return {{feedbackMail: {href: string, target: string, title: string}}}
   * @override
   */
  model(): IApplicationRouteModel {
    const [
      showStagingBanner,
      showLiveDataWarning,
      showChangeManagement,
      avatarEntityProps,
      wikiLinks,
      customBanner,
      applicationSupportEmail
    ] = [
      getConfig('isStagingBanner', { useDefault: true, default: false }),
      getConfig('isLiveDataWarning', { useDefault: true, default: false }),
      getConfig('showChangeManagement', { useDefault: true, default: false }),
      getConfig('userEntityProps'),
      getConfig('wikiLinks'),
      getConfig('customBanner'),
      getConfig('applicationSupportEmail', { useDefault: true, default: '' })
    ];

    const { dataModels: dataModelsService, avatar: avatarService } = this;
    const PersonEntityClass = dataModelsService.getModel('people');

    /**
     * properties for the navigation link to allow a user to provide feedback
     */
    const feedbackMail: IMailHeaderRecord & { title: string } = {
      title,
      subject,
      to: applicationSupportEmail
    };

    // These properties are saved on the person entity class, but are gotten through a config so
    // they need to be loaded here to be used in the future.
    // Note: This is now deprecated in favor of using the Avatar class and will be removed as we
    // finish implementing avatars
    PersonEntityClass.aviUrlPrimary = avatarEntityProps.aviUrlPrimary;
    PersonEntityClass.aviUrlFallback = avatarEntityProps.aviUrlFallback;

    avatarService.initWithConfigs(avatarEntityProps);

    return {
      feedbackMail,
      showStagingBanner,
      showLiveDataWarning,
      showChangeManagement,
      customBanner,
      helpResources: [{ link: wikiLinks['appHelp'], label: 'DataHub Wiki' }]
    };
  }
  /**
   * Attempt to load the current user and application configuration options
   * @returns {Promise}
   */
  beforeModel(...args: Array<unknown>): Promise<[void, IAppConfig]> {
    return super.beforeModel.apply(this, args);
  }

  /**
   * Perform post model operations
   * @return {Promise}
   */
  afterModel(...args: Array<unknown>): Promise<[void, false | void]> {
    super.afterModel.apply(this, args);

    return Promise.all([this._setupMetricsTrackers(), this._trackCurrentUser()]);
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

  /**
   * At a more granular level, initializing the banner before the render loop of the entire page ends will results in the
   * render loop of the application breaking the css transition animation for our initial banners. This hook is being used
   * to schedule banners only after initial render has taken place in order to allow users see the banner animation
   * on entry
   */
  renderTemplate(controller: Controller, model: Model): void {
    super.renderTemplate.apply(this, [controller, model]);
    const { showStagingBanner, showLiveDataWarning, showChangeManagement, customBanner } = model;
    const { banners } = this;
    scheduleOnce('afterRender', this, (): void => {
      banners.appInitialBanners([showStagingBanner, showLiveDataWarning, showChangeManagement]);
      if (customBanner && customBanner.showCustomBanner) {
        const { content, type, icon, link } = customBanner;
        banners.addCustomBanner(content, type as NotificationEvent, icon, link);
      }
    });
  }
}
