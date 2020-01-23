import { resolveDynamicRouteName } from '@datahub/utils/routes/routing';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';
import { MaybeRouteInfoWithAttributes } from '@datahub/utils/types/vendor/routerjs';
import { listOfEntitiesMap } from '@datahub/data-models/entity/utils/entities';
import Transition from '@ember/routing/-private/transition';
import { DataModelEntity } from '@datahub/data-models/constants/entity';

/**
 * Indexes the route names we care about to functions that resolve the placeholder value
 * defaults to the route.name, if a resolved value cannot be determined
 * @type Record<string, ((r: RouteInfoWithOrWithoutAttributes) => string) | undefined>
 */
export const mapOfRouteNamesToResolver: Record<string, ((r: MaybeRouteInfoWithAttributes) => string) | void> = {
  'browse.entity': (route: MaybeRouteInfoWithAttributes): string =>
    route.attributes ? `browse.${route.attributes.entity}` : route.name,
  'browse.entity.index': (route: MaybeRouteInfoWithAttributes): string =>
    route.attributes ? `browse.${route.attributes.entity}` : route.name,
  'datasets.dataset.tab': (route: MaybeRouteInfoWithAttributes): string =>
    route.attributes ? `${DatasetEntity.displayName}.${route.attributes.currentTab}` : route.name
};

/**
 * Guard checks that a route name is an entity route by testing if the routeName begins with the entity name
 * @param {string} routeName the name of the route to check against
 * @returns {boolean}
 */
const routeNameIsEntityRoute = (routeName: string): boolean =>
  listOfEntitiesMap((e): DataModelEntity['displayName'] => e.displayName).some(
    (entityName: DataModelEntity['displayName']): boolean => routeName.startsWith(entityName)
  );

/**
 * Check if the route info instance has a name that is considered an entity route
 * @returns {boolean}
 */
export const isRouteEntityPageRoute = (routeBeingTransitionedTo: Transition['to' | 'from']): boolean => {
  const routeName = resolveDynamicRouteName(mapOfRouteNamesToResolver, routeBeingTransitionedTo);
  return Boolean(routeName && routeNameIsEntityRoute(routeName));
};
