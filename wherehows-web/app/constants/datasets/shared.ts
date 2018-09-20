import Route from '@ember/routing/route';
import { assert } from '@ember/debug';
import { set } from '@ember/object';
import DatasetController from 'wherehows-web/controllers/datasets/dataset';

/**
 * Defines id strings for page tabs available on the dataset page,
 * these also match up with their respective route names
 * @type {string}
 */
enum Tabs {
  Properties = 'properties',
  Access = 'access',
  Comments = 'comments',
  Schema = 'schema',
  Ownership = 'ownership',
  Compliance = 'compliance',
  SampleData = 'sample',
  Relationships = 'relationships',
  Health = 'health'
}

/**
 * Sets a DatasetController property K to the supplied value
 * @template K keyof DatasetController
 * @param {Route} route the route instance to update
 * @param {K} prop the property on the DatasetController to be set
 * @param {DatasetController[K]} value value to be applied to prop
 * @returns {DatasetController[K]}
 */
const setPropertyOnAncestorController = <K extends keyof DatasetController>(
  route: Route,
  prop: K,
  value: DatasetController[K]
): DatasetController[K] => {
  const { routeName, controllerFor } = route;
  assert('route should be a descendant of datasets.dataset', !routeName.indexOf('datasets.dataset.'));
  const ancestorController = <DatasetController>controllerFor.call(route, 'datasets.dataset');

  return set(ancestorController, prop, value);
};

/**
 * Factory creates a dataset Route class that sets the currently selected tab on the parent controller
 * @param {{ selectedTab: Tabs }} { selectedTab } options bag contains identifier for the current tab
 * @returns {typeof Route} the descendant route class
 */
const descendantDatasetRouteClassFactory = ({ selectedTab }: { selectedTab: Tabs }): typeof Route =>
  class DatasetDescendantRoute extends Route.extend({
    actions: {
      didTransition(this: DatasetDescendantRoute): void {
        // on successful route transition
        setPropertyOnAncestorController(this, 'tabSelected', selectedTab);
      }
    }
  }) {};

export { Tabs, descendantDatasetRouteClassFactory, setPropertyOnAncestorController };
