import RouteInfo from '@ember/routing/-private/route-info';

/**
 * Extends the RouteInfo interface with the attribute attributes which contains
 * parameters from the current transition route
 * @export
 * @interface MaybeRouteInfoWithAttributes
 * @extends {RouteInfo}
 */
// eslint-disable-next-line @typescript-eslint/interface-name-prefix
export interface MaybeRouteInfoWithAttributes extends RouteInfo {
  attributes?: any; // eslint-disable-line @typescript-eslint/no-explicit-any
}
