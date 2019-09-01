import { arraySome } from 'wherehows-web/utils/array';
import Transition, { RouteInfoWithAttributes } from 'wherehows-web/typings/modules/routerjs';
import { RouteInfoWithOrWithoutAttributes } from 'wherehows-web/typings/app/routes';
import { listOfEntitiesMap } from '@datahub/data-models/entity/utils/entities';
import { Route } from '@ember/routing';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';

/**
 * Type guard for RouteInfoWithOrWithoutAttributes union type
 * @param {RouteInfoWithOrWithoutAttributes} routeInfo
 * @returns {routeInfo is RouteInfoWithAttributes}
 */
export const isRouteInfoWithAttributes = (
  routeInfo: RouteInfoWithOrWithoutAttributes
): routeInfo is RouteInfoWithAttributes => routeInfo.hasOwnProperty('attributes');

/**
 * Indexes the route names we care about to functions that resolve the placeholder value
 * defaults to the route.name, if a resolved value cannot be determined
 * @type Record<string, ((r: RouteInfoWithOrWithoutAttributes) => string) | undefined>
 */
export const mapOfRouteNamesToResolver: Record<string, ((r: RouteInfoWithOrWithoutAttributes) => string) | void> = {
  'browse.entity': (route: RouteInfoWithOrWithoutAttributes): string =>
    isRouteInfoWithAttributes(route) ? `browse.${route.attributes.entity}` : route.name,
  'browse.entity.index': (route: RouteInfoWithOrWithoutAttributes): string =>
    isRouteInfoWithAttributes(route) ? `browse.${route.attributes.entity}` : route.name,
  'datasets.dataset.tab': (route: RouteInfoWithOrWithoutAttributes): string =>
    isRouteInfoWithAttributes(route) ? `${DatasetEntity.displayName}.${route.attributes.currentTab}` : route.name
};

/**
 * Takes a RouteInfo to resolver function mapping and a RouteInfo instance, resolves a route name
 * Some routes are shared between different but related features. This is done via placeholders.
 * This provides a way to resolve the meaningful value of the placeholder segments.
 * @param {(Record<string, ((r: RouteInfoWithOrWithoutAttributes) => string) | void>)} routeResolverMap
 * @param {RouteInfoWithOrWithoutAttributes | null} routeBeingTransitionedTo route info object for the route being navigated to
 * @returns {string | null}
 */
export const resolveDynamicRouteName = (
  routeResolverMap: Record<string, ((r: RouteInfoWithOrWithoutAttributes) => string) | void>,
  routeBeingTransitionedTo?: RouteInfoWithOrWithoutAttributes | null
): string | null => {
  if (routeBeingTransitionedTo) {
    const routeName = routeBeingTransitionedTo.name;
    const resolveRouteName = routeResolverMap[routeName];

    return typeof resolveRouteName === 'function' ? resolveRouteName(routeBeingTransitionedTo) : routeName;
  }

  return null;
};

/**
 * Guard checks that a route name is an entity route by testing if the routeName begins with the entity name
 * @param {string} routeName the name of the route to check against
 * @returns {boolean}
 */
const routeNameIsEntityRoute = (routeName: string): boolean =>
  arraySome((entityName: string): boolean => routeName.startsWith(entityName))(listOfEntitiesMap(e => e.displayName));

/**
 * Check if the route info instance has a name that is considered an entity route
 * @param {Transition<Route>['to']} routeBeingTransitionedTo
 * @returns {boolean}
 */
export const isRouteEntityPageRoute = (routeBeingTransitionedTo: Transition<Route>['to']): boolean => {
  const routeName = resolveDynamicRouteName(mapOfRouteNamesToResolver, routeBeingTransitionedTo);
  return Boolean(routeName && routeNameIsEntityRoute(routeName));
};
