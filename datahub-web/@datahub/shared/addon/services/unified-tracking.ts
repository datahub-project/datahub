import Service from '@ember/service';
import { inject as service } from '@ember/service';
import Metrics, { IAdapterOptions } from 'ember-metrics';
import CurrentUser from '@datahub/shared/services/current-user';
import { ITrackingConfig } from '@datahub/shared/types/configurator/tracking';
import { ITrackSiteSearchParams } from '@datahub/shared/types/tracking/search';
import { getPiwikActivityQueue } from '@datahub/shared/utils/tracking/piwik';
import { scheduleOnce } from '@ember/runloop';
import RouterService from '@ember/routing/router-service';
import { alias } from '@ember/object/computed';
import Transition from '@ember/routing/-private/transition';
import { resolveDynamicRouteName } from '@datahub/utils/routes/routing';
import { mapOfRouteNamesToResolver } from '@datahub/data-models/utils/entity-route-name-resolver';
import { searchRouteName } from '@datahub/shared/constants/tracking/site-search-tracking';
import RouteInfo from '@ember/routing/-private/route-info';
import {
  IBaseTrackingEvent,
  IBaseTrackingGoal,
  ICustomEventData,
  IPageViewEventTrackingParams
} from '@datahub/shared/types/tracking/event-tracking';
import { PageType, TrackingEventCategory } from '@datahub/shared/constants/tracking/event-tracking';
import FoxieService from '@datahub/shared/services/foxie';
import { UserFunctionType } from '@datahub/shared/constants/foxie/user-function-type';

/**
 * Defines the base and full api for the analytics / tracking module in DataHub
 * @export
 * @class UnifiedTracking
 */
export default class UnifiedTracking extends Service {
  /**
   * References the Ember Metrics addon service, which serves as a proxy to analytics services for
   * metrics collection within the application
   */
  @service
  metrics!: Metrics;

  /**
   * Injection of the foxie service to trigger events from all tracked items as well, saves some effort in firing foxie
   * events
   */
  @service
  foxie!: FoxieService;

  /**
   * Injected reference to the shared CurrentUser service, user here to inform the analytics service of the currently logged in
   * user
   */
  @service
  currentUser!: CurrentUser;

  /**
   * Injects a reference to the router service, used to handle application routing concerns such as event handler binding
   */
  @service
  router!: RouterService;

  /**
   * The current user's username
   */
  @alias('currentUser.entity.username')
  userName?: string;

  init(): void {
    super.init();

    // On init ensure that page view transitions are captured by the metrics services
    this.trackPageViewOnRouteChange();
  }

  /**
   * If tracking is enabled, activates the adapters for the applicable analytics services
   * @param {ITrackingConfig} tracking a configuration object with properties for enabling tracking or specifying behavior
   */
  setupTrackers(tracking: ITrackingConfig): Array<IAdapterOptions> {
    const adapterOptions: Array<IAdapterOptions> = [];

    if (tracking.isEnabled) {
      const metrics = this.metrics;
      const { trackers } = tracking;
      const { piwikSiteId, piwikUrl } = trackers.piwik;
      const trackingAdaptersToActivate: Array<IAdapterOptions> = [
        ...adapterOptions,
        {
          name: 'Piwik',
          environments: ['all'],
          config: {
            piwikUrl,
            siteId: piwikSiteId
          }
        }
      ];

      if (piwikSiteId && piwikUrl) {
        // Only activate adapters if the above two parameters are defined, otherwise ember metrics will throw an error
        metrics.activateAdapters(trackingAdaptersToActivate);
      }

      return trackingAdaptersToActivate;
    }

    return adapterOptions;
  }

  /**
   * Identifies and sets the currently logged in user to be tracked on the activated analytics services
   * @param {ITrackingConfig} tracking a configuration object with properties for enabling tracking or specifying behavior
   */
  setCurrentUser(tracking: ITrackingConfig): void {
    const { currentUser, metrics } = this;

    // Check if tracking is enabled prior to tracking the current user
    // Passes an anonymous function to track the currently logged in user using the `current-user` service CurrentUser
    tracking.isEnabled && currentUser.trackCurrentUser((userId: string): void => metrics.identify({ userId }));
  }

  /**
   * This tracks the search event when a user successfully requests a search query
   * @param {ITrackSiteSearchParams} { keyword, entity, searchCount } parameters for the search operation performed by the user
   */
  trackSiteSearch({ keyword, entity, searchCount }: ITrackSiteSearchParams): void {
    getPiwikActivityQueue().push(['trackSiteSearch', keyword, entity, searchCount]);
  }

  /**
   * Tracks application events that are not site search events or page view. These are typically custom events that occur as
   * a user interacts with the app
   * @template T types that match the IBaseTrackingEvent or a specialized extension
   * @template O optional type of attributes that may be used in constructing the tracking event
   * @param {T} eventDetails properties to be used in building the tracking event
   * @param {O} [options] optional attributes that may be used in constructing the tracking event
   */
  trackEvent<T extends IBaseTrackingEvent, O extends ICustomEventData>(eventDetails: T, options?: O): void {
    this.metrics.trackEvent(this.buildEvent(eventDetails, options));
    this.foxie.launchUFO({
      functionType:
        eventDetails.category === TrackingEventCategory.ControlInteraction
          ? UserFunctionType.Interaction
          : UserFunctionType.Tracking,
      functionTarget: eventDetails.action,
      functionContext: (eventDetails.name || '') + (eventDetails.value || '')
    });
  }

  /**
   * Constructs the resolved event options to be passed into the ember-metrics service
   * @template T
   * @param {IBaseTrackingEvent} baseEventAttrs the attributes to be used in constructing the base event
   * @param {T} [_options] optional attributes that may be used in constructing the tracking event
   */
  buildEvent<T extends ICustomEventData>(baseEventAttrs: IBaseTrackingEvent, _options?: T): IBaseTrackingEvent {
    const { category, action, name, value } = baseEventAttrs;
    const resolvedOptions = Object.assign({}, { category, action }, !!name && { name }, !!value && { value });

    return resolvedOptions;
  }

  /**
   * Track when a goal is met by adding the goal identifier to the activity queue
   * @param {IBaseTrackingGoal} goal the goal to be tracked
   */
  trackGoal(goal: IBaseTrackingGoal): void {
    getPiwikActivityQueue().push(['trackGoal', goal.name]);
  }

  /**
   * Tracks impressions for all rendered DOM content
   * This is scheduled in the afterRender queue to ensure that tracking services can accurately identify content blocks
   * that have been tagged with data-track-content data attributes. This methodology is currently specific to Piwik tracking
   */
  trackContentImpressions(): void {
    const trackOnPaq = (): number => getPiwikActivityQueue().push(['trackAllContentImpressions']);
    scheduleOnce('afterRender', null, trackOnPaq);
  }

  /**
   * Binds the handler to track page views on route change
   */
  trackPageViewOnRouteChange(): void {
    // Bind to the routeDidChange event to track global successful route transitions and track page view on the metrics service
    this.router.on('routeDidChange', ({ to }: Transition): void => {
      const { router, metrics } = this;
      // Fetch the URL from the router and store it as an identifier
      // Note: We need to append the hash '#' here as this isn't included in our tracking adapter but we need this hash
      // to provide accurate information about the page since the Ember app uses it
      const page: string = '/#' + router.currentURL;
      // fallback to page value if a resolution cannot be determined, e.g when to / from is null
      const title: string = resolveDynamicRouteName(mapOfRouteNamesToResolver, to) || page || '';

      // Determine if the currentURL belongs to that of a searchPage
      const isSearchRoute: boolean =
        title.includes(searchRouteName) ||
        Boolean(to && to.find(({ name }: RouteInfo): boolean => name === searchRouteName));
      // Default the pageType to that of only `Router` since  we fetch the URL directly from Ember Router.
      const pageType: PageType = PageType.Router;
      // Fetch current user's identifier from the currentUser service
      const { userName = '' } = this;

      // Track a page view event only on page's / route's that are not search
      // and there is a page to track
      if (!isSearchRoute && page) {
        const pageViewEventParams: IPageViewEventTrackingParams = {
          page,
          title,
          pageType,
          userName
        };
        metrics.trackPage(pageViewEventParams);
        this.foxie.launchUFO({
          functionType: UserFunctionType.Navigation,
          functionTarget: page,
          functionContext: title
        });
      }

      getPiwikActivityQueue().push(['enableHeartBeatTimer']);
    });
  }
}

declare module '@ember/service' {
  // eslint-disable-next-line @typescript-eslint/interface-name-prefix
  interface Registry {
    'unified-tracking': UnifiedTracking;
  }
}
