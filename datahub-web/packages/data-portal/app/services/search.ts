import TrackingService from './tracking';
import { inject as service } from '@ember/service';
import RouterService from '@ember/routing/router-service';
import DwellTime from 'wherehows-web/utils/analytics/search/dwell-time';
import { SuccessfulDwellTimeLength, searchRouteName } from 'wherehows-web/constants/analytics/site-search-tracking';
import Transition, { RouteInfo } from 'wherehows-web/typings/modules/routerjs';
import { Route } from '@ember/routing';
import { Time } from '@datahub/metadata-types/types/common/time';
import { action } from '@ember/object';
import { searchTrackingEvent } from 'wherehows-web/constants/analytics/event-tracking/search';
import { isRouteEntityPageRoute } from 'wherehows-web/utils/helpers/routes';
import { DataModelName } from '@datahub/data-models/constants/entity';
import Service from '@ember/service';

/**
 * Search service is used to maintain the same
 * state on different parts of the app. Right now the only thing
 * we need to persist is the keywords.
 */
export default class Search extends Service {
  /**
   * Keyword or query for the current search
   */
  keyword: string;

  /**
   * Current entity of the search
   */
  entity?: DataModelName;

  /**
   * Optional urn of the clicked search result
   * @type {string}
   */
  private clickedSearchResultUrn?: string;

  /**
   * On service instantiation a reference to a DwellTime instance is cached here to track dwell time
   * @type {DwellTime}
   * @memberof Search
   */
  dwellTimeTracker: DwellTime;

  /**
   * References the application service to track user events, metrics and interaction
   * @type {TrackingService}
   * @memberof Search
   */
  @service
  tracking: TrackingService;

  /**
   * Router service used in tracking dwell time
   * @type {(RouterService & { currentRoute: RouteInfo })}
   * @memberof Search
   */
  @service
  router: RouterService & { currentRoute: RouteInfo };

  /**
   * Performs a set of operation when a search result is clicked
   * @param {string} urn
   * @returns {void}
   * @memberof Search
   */
  didClickSearchResult(urn: string): void {
    // When a search result is clicked, initiate dwell time tracking
    this.dwellTimeTracker.beginTracking();
    // Also, track event as a click event
    this.clickedSearchResultUrn = urn;
    this.tracking.trackEvent({ ...searchTrackingEvent.SearchResultClick, name: this.clickedSearchResultUrn });
  }

  /**
   * Sets the threshold value of the successful amount of dwell time on the component
   * @instance
   */
  successfulDwellTimeLength = SuccessfulDwellTimeLength;

  /**
   * Track SAT Click as an event
   * This is inferred based on
   * 1) Time spent or more specifically dwell time on viewing the entity page (see DwellTime class for differentiation)
   * @link {DwellTime}
   * 2) navigation sequence i.e. click from a search implied by dwell time, that lands on an entity page
   * @param {number} dwellTime
   * @returns boolean
   */
  @action
  trackSatClick(dwellTime: number, transition: Transition<Route>): boolean {
    const isSATClick = this.isSATClick(dwellTime, transition);

    if (isSATClick) {
      this.tracking.trackEvent({ ...searchTrackingEvent.SearchResultSATClick, name: this.clickedSearchResultUrn });
    }

    return isSATClick;
  }

  init(): void {
    super.init();

    // Instantiate a DwellTime instance to track dwell time, which is essentially time spent on a search result page
    this.dwellTimeTracker = new DwellTime(searchRouteName, this.router, this.trackSatClick);
  }

  /**
   * Determines if the click can be judged as a satisfied click
   * @param {Time} dwellTime the amount of time in milliseconds the user has dwelled on the page
   * @param {Transition<Route>} { to, from } the current ember transition object
   * @returns boolean
   * @memberof Search
   */
  isSATClick(dwellTime: Time, { to, from }: Transition<Route>): boolean {
    const isReturningToSearch = Boolean(to && to.name === searchRouteName);
    // If the user is returning to the search page, check if the dwellTime is meets or exceeds what is considered a successful amount of time
    const isSufficientDwellTimeOnSearchReturn = isReturningToSearch && dwellTime >= this.successfulDwellTimeLength;

    // If the dwell time is sufficient and the user is returning to search, or the user is navigating away from an entity route
    // to a route that is not search this is considered a satisfied click
    return isSufficientDwellTimeOnSearchReturn || (isRouteEntityPageRoute(from) && !isReturningToSearch);
  }
}
