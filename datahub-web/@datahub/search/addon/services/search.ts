import {
  ISearchResultClickTrackEventParams,
  ICustomEventData,
  IBaseTrackingEvent
} from '@datahub/shared/types/tracking/event-tracking';
import { inject as service } from '@ember/service';
import RouterService from '@ember/routing/router-service';
import DwellTime from '@datahub/shared/utils/tracking/dwell-time';
import { Time } from '@datahub/metadata-types/types/common/time';
import { action } from '@ember/object';
import { searchTrackingEvent } from '@datahub/shared/constants/tracking/event-tracking/search';
import Service from '@ember/service';
import { isRouteEntityPageRoute } from '@datahub/data-models/utils/entity-route-name-resolver';
import { SuccessfulDwellTimeLength, searchRouteName } from '@datahub/shared/constants/tracking/site-search-tracking';
import Transition from '@ember/routing/-private/transition';
import UnifiedTracking from '@datahub/shared/services/unified-tracking';
import {
  PageKey,
  CustomTrackingEventName,
  SearchActionEventCategory
} from '@datahub/shared/constants/tracking/event-tracking';
import CurrentUser from '@datahub/shared/services/current-user';
import DataModelsService from '@datahub/data-models/services/data-models';
import { DataModelName } from '@datahub/data-models/constants/entity';

/**
 * Search service is used to maintain the same
 * state on different parts of the app. Right now the only thing
 * we need to persist is the keywords.
 */
export default class SearchService extends Service {
  /**
   * Keyword or query for the current search
   */
  keyword!: string;

  /**
   * Data Models service to fetch unguarded entities
   */
  @service('data-models')
  dataModels!: DataModelsService;

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
  dwellTimeTracker!: DwellTime;

  /**
   * Injects the application CurrentUser service to provide the actor information on the action event
   */
  @service('current-user')
  sessionUser!: CurrentUser;

  /**
   * References the application service to track user events, metrics and interaction
   */
  @service('unified-tracking')
  tracking!: UnifiedTracking;

  /**
   * Router service used in tracking dwell time
   */
  @service
  router!: RouterService;

  /**
   * Performs a set of operation when a search result is clicked.
   *
   * Responsible for gathering the required parameters for tracking the click and passes them onto the `trackSearchCategoryEvent` method.
   * @param {string} urn The urn associated with the clicked search result
   * @param {number} absolutePosition The position of the specific result in regards with all the search-results
   */
  didClickSearchResult(urn: string, absolutePosition: number): void {
    // When a search result is clicked, initiate dwell time tracking
    this.dwellTimeTracker.beginTracking();
    // Also, track event as a click event
    this.clickedSearchResultUrn = urn;
    const searchResultEventParams: ISearchResultClickTrackEventParams = {
      baseSearchTrackingEvent: searchTrackingEvent.SearchResultClick,
      actionCategory: SearchActionEventCategory.SearchClick,
      absolutePosition,
      facet: null
    };
    this.trackSearchActionEvent(searchResultEventParams);
  }

  /**
   * Sets the threshold value of the successful amount of dwell time on the component
   * @instance
   */
  successfulDwellTimeLength = SuccessfulDwellTimeLength;

  /**
   * Track SAT Click as an event
   * 1) Time spent or more specifically dwell time on viewing the entity page (see DwellTime class for differentiation)
   * @link {DwellTime}
   * 2) navigation sequence i.e. click from a search implied by dwell time, that lands on an entity page
   * @param {number} dwellTime
   * @returns boolean
   */
  @action
  trackSatClick(dwellTime: number, transition: Transition): boolean {
    const isSATClick = this.isSATClick(dwellTime, transition);
    if (isSATClick) {
      this.trackSearchActionEvent({
        baseSearchTrackingEvent: searchTrackingEvent.SearchResultSATClick,
        actionCategory: SearchActionEventCategory.SearchSatClick,
        absolutePosition: null,
        facet: null
      });
    }

    return isSATClick;
  }

  /**
   * Creates attributes for tracking the search action event and invokes the tracking service.
   *
   * @param {ISearchResultClickTrackEventParams} searchResultEventParams A map consisting the current user's username, search query keyed in by the user , the positioning of the search result card in the list, the facet filters clicked by the user
   */
  trackSearchActionEvent(searchActionEventParams: ISearchResultClickTrackEventParams): void {
    const { baseSearchTrackingEvent, absolutePosition, actionCategory, facet } = searchActionEventParams;
    const userName = this.sessionUser.entity?.username || '';

    const baseEventAttrs: IBaseTrackingEvent = {
      ...baseSearchTrackingEvent,
      name: this.clickedSearchResultUrn
    };
    const customEventAttrs: ICustomEventData = {
      pageKey: PageKey.searchCategory,
      eventName: CustomTrackingEventName.SearchAction,
      userName,
      target: this.clickedSearchResultUrn,
      body: {
        requesterUrn: userName,
        targetUrn: this.clickedSearchResultUrn,
        actionCategory,
        query: this.keyword,
        absolutePosition,
        facet
      }
    };
    this.tracking.trackEvent(baseEventAttrs, customEventAttrs);
  }

  init(): void {
    super.init();

    // Instantiate a DwellTime instance to track dwell time, which is essentially time spent on a search result page
    this.dwellTimeTracker = new DwellTime(searchRouteName, this.router, this.trackSatClick);
  }

  /**
   * Determines if the click can be judged as a satisfied click
   * @param {Time} dwellTime the amount of time in milliseconds the user has dwelled on the page
   * @param {Transition} { to, from } the current ember transition object containing the RouteInfo object being transitioned to or from
   */
  isSATClick(dwellTime: Time, { to, from }: Transition): boolean {
    const isReturningToSearch = Boolean(to && to.name === searchRouteName);
    // If the user is returning to the search page, check if the dwellTime is meets or exceeds what is considered a successful amount of time
    const isSufficientDwellTimeOnSearchReturn = isReturningToSearch && dwellTime >= this.successfulDwellTimeLength;

    // If the dwell time is sufficient and the user is returning to search, or the user is navigating away from an entity route
    // to a route that is not search this is considered a satisfied click
    return (
      isSufficientDwellTimeOnSearchReturn ||
      (isRouteEntityPageRoute(from, this.dataModels.guards.unGuardedEntitiesDisplayName) && !isReturningToSearch)
    );
  }
}

// DO NOT DELETE: this is how TypeScript knows how to look up your services.
declare module '@ember/service' {
  // This is a core ember thing
  //eslint-disable-next-line @typescript-eslint/interface-name-prefix
  interface Registry {
    search: SearchService;
  }
}
