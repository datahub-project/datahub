import Service from '@ember/service';
import { inject as service } from '@ember/service';
import Metrics from 'ember-metrics';
import CurrentUser from '@datahub/shared/services/current-user';
import { ITrackingConfig } from '@datahub/shared/types/configurator/tracking';
import { ITrackSiteSearchParams } from '@datahub/tracking/types/search';
import { getPiwikActivityQueue } from '@datahub/tracking/utils/piwik';
import { scheduleOnce } from '@ember/runloop';
import RouterService from '@ember/routing/router-service';
import Transition from '@ember/routing/-private/transition';
import { resolveDynamicRouteName } from '@datahub/utils/routes/routing';
import { mapOfRouteNamesToResolver } from '@datahub/data-models/utils/entity-route-name-resolver';
import { searchRouteName } from '@datahub/tracking/constants/site-search-tracking';
import RouteInfo from '@ember/routing/-private/route-info';
import { IBaseTrackingEvent, IBaseTrackingGoal } from '@datahub/tracking/types/event-tracking';

/**
 * Defines the base and full api for the analytics / tracking module in Data Hub
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

  init(): void {
    super.init();

    // On init ensure that page view transitions are captured by the metrics services
    this.trackPageViewOnRouteChange();
  }

  /**
   * If tracking is enabled, activates the adapters for the applicable analytics services
   * @param {ITrackingConfig} tracking a configuration object with properties for enabling tracking or specifying behavior
   */
  setupTrackers(tracking: ITrackingConfig): void {
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
   */
  trackEvent(event: IBaseTrackingEvent): void {
    const { category, action, name, value } = event;
    const resolvedOptions = Object.assign({}, { category, action }, !!name && { name }, !!value && { value });

    this.metrics.trackEvent(resolvedOptions);
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
    void scheduleOnce('afterRender', null, (): number => getPiwikActivityQueue().push(['trackAllContentImpressions']));
  }

  /**
   * Binds the handler to track page views on route change
   */
  trackPageViewOnRouteChange(): void {
    // Bind to the routeDidChange event to track global successful route transitions and track page view on the metrics service
    this.router.on('routeDidChange', ({ to }: Transition): void => {
      const { router, metrics } = this;
      const page = router.currentURL;
      // fallback to page value if a resolution cannot be determined, e.g when to / from is null
      const title = resolveDynamicRouteName(mapOfRouteNamesToResolver, to) || page;
      const isSearchRoute =
        title.includes(searchRouteName) || (to && to.find(({ name }: RouteInfo): boolean => name === searchRouteName));

      if (!isSearchRoute) {
        // Track a page view event only on page's / route's that are not search
        metrics.trackPage({ page, title });
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
