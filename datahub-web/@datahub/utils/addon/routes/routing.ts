import { MaybeRouteInfoWithAttributes } from '@datahub/utils/types/vendor/routerjs';

/**
 * Takes a RouteInfo to resolver function mapping and a RouteInfo instance, resolves a route name
 * Some routes are shared between different but related features. This is done via placeholders.
 * This provides a way to resolve the meaningful value of the placeholder segments.
 * @param {(Record<string, ((r: MaybeRouteInfoWithAttributes) => string) | void>)} routeResolverMap a map of route names to
 * functions that parse the route name to a common string for example, datasets.dataset.tab route => datasets.schema,
 * where schema is the current tab in this instance
 * @param {MaybeRouteInfoWithAttributes | null} routeBeingTransitionedTo route info object for the route being navigated to
 * @returns {string | null}
 */
export const resolveDynamicRouteName = (
  routeResolverMap: Record<string, ((r: MaybeRouteInfoWithAttributes) => string) | void>,
  routeBeingTransitionedTo?: MaybeRouteInfoWithAttributes | null
): string | null => {
  if (routeBeingTransitionedTo) {
    const routeName = routeBeingTransitionedTo.name;
    const resolveRouteName = routeResolverMap[routeName];

    return typeof resolveRouteName === 'function' ? resolveRouteName(routeBeingTransitionedTo) : routeName;
  }

  return null;
};
