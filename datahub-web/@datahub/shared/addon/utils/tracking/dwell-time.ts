import RouterService from '@ember/routing/router-service';
import { Time } from '@datahub/metadata-types/types/common/time';
import { action } from '@ember/object';
import Transition from '@ember/routing/-private/transition';

const ROUTE_EVENT_NAME = 'routeDidChange';

/**
 * Dwell time is defined as the time between when a user clicks on a search result
 * and when they leave the search result page to return "back" to search.
 * This is a different metric from "time on page", which is the time the user
 * spends on a page before going somewhere else.
 * Key differentiation being the return to the search page.
 * However, to align with Flagship, a modified inference for Dwell time is implemented here,
 * which is essentially the time on page metric since the user does not have to return to search
 *
 * The exit navigation sequences that will be recorded as Dwell time
 * 1) user navigates to another route from the entity page
 * 2) user closes the browser tab / window
 *
 * @export
 * @class DwellTime
 */
export default class DwellTime {
  /**
   * The name of the search route to trigger the start of dwell time recording on exit from route
   * @type {string}
   * @instance
   */
  searchRouteName: string;

  /**
   * The time at which user's dwell time will be counted from, this is the time the user exits the search route
   * to an entity route
   * @type {Time}
   * @instance
   */
  startTime: Time = 0;

  /**
   * The time a user spends dwelling on a page till they exit the page, constrained by navigation sequence
   * explained above
   * @type {Time}
   * @instance
   */
  dwellTime: Time = 0;

  /**
   * Callback function to be invoked when dwell time is recorded. Boolean value returned is used as a
   * guard in determining if the current dwell time being measured should be reset
   * @type {(dwellTime: Time) => boolean}
   * @instance
   */
  didDwell?: (dwellTime: Time, transition: Transition) => boolean;

  /**
   * Retains a reference to the last seen transition object
   * @private
   * @type {Transition<Route>}
   * @instance
   */
  private lastTransition?: Transition;

  /**
   *Creates an instance of DwellTime.
   * @param {string} searchRouteName the name of the search that will trigger a dwell time recording
   * @param {(RouterService & {
   *       off?: (ev: string, cb: Function) => any;
   *       on?: (ev: string, cb: Function) => any;
   *       currentRoute: RouteInfo;
   *     })} route a reference to the router service used to listen for navigation / router transition events
   * @param {DwellTime['didDwell']} [didDwell] callback function invoked when dwell time is recorded
   * @memberof DwellTime
   */
  constructor(
    searchRouteName: string,
    readonly route: RouterService & {
      off?: (ev: string, cb: Function) => unknown;
      on?: (ev: string, cb: Function) => unknown;
    },
    didDwell?: DwellTime['didDwell']
  ) {
    this.searchRouteName = searchRouteName;
    this.didDwell = didDwell;

    // Bind handler to successful route transition events
    route.on && route.on(ROUTE_EVENT_NAME, this.onRouteChange);

    // According to ember docs, the RouteService extends a Service, therefore
    // it has a willDestroy hook that we can wrap to autoclean DwellTime
    const willDestroy = route.willDestroy;
    route.willDestroy = (): void => {
      this.onDestroy();
      willDestroy.call(route);
    };
  }

  /**
   * Assigns the time at which the user transitions to a search result, the difference between this and
   * the subsequently measured time will indicate the dwell time
   * @return {void}
   * @memberof DwellTime
   */
  beginTracking(): void {
    this.startTime = Date.now();
  }

  /**
   * Reset the dwell time tracking for this instance. The start time greater than 0 indicates that a
   * tracking sequence is in effect
   * @return {void}
   * @memberof DwellTime
   */
  resetDwellTimeTracking(): void {
    this.startTime = 0;
  }

  /**
   * Handles the transition events on the ember router service object to begin or record dwell time
   * @private
   * @returns {void}
   * @instance
   */
  private onRouteChange = (transition: Transition & { urlMethod: 'update' | 'replace' | null }): void => {
    this.lastTransition = transition;
    // TODO META-11678 Although replacing transitions should be transparent to the user
    // the desirable effect is to capture the last transition of the 'replace' route chain.
    // There is not a straight forward way of doing this at the moment.
    // Replace transition should not be tracked as it is transparent to the user
    if (transition.urlMethod !== 'replace' && transition.to) {
      this.record(transition);
    }
  };

  /**
   * Records the total dwell time and invokes the supplied optional callback
   * @param {Transition<Route>} transition
   * @returns {Time}
   * @memberof DwellTime
   */
  record(transition: Transition): Time {
    const { startTime } = this;

    // Check if dwell time is already being measured, indicated by a non-zero start time
    if (startTime) {
      const dwellTime = (this.dwellTime = Date.now() - startTime);
      // The callback's execution boolean value will determine if measuring should be terminated
      // otherwise, if no callback, terminate the recording until a new transition
      // happens from a route with `searchRouteName`
      const shouldStopRecording = this.didDwell ? this.didDwell(dwellTime, transition) : false;

      if (shouldStopRecording) {
        this.resetDwellTimeTracking();
      }
    }

    return this.dwellTime;
  }

  /**
   * Clean up operations when this instance is being destroyed
   * If a dwell time is currently being recorded, completed the recording before exit
   * annotated with @action for auto-binding to instance
   */
  @action
  onDestroy(): void {
    const { lastTransition } = this;

    if (lastTransition) {
      this.record(lastTransition);
    }

    this.unbindListeners();
  }

  /**
   * Removes bindings to listener functions to allow GC and clean up memory.
   * @private
   * @memberof DwellTime
   */
  unbindListeners(): void {
    if (this.route.off) {
      this.route.off(ROUTE_EVENT_NAME, this.onRouteChange);
    }
  }
}
