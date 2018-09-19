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
 * Sets the tab selection property on the provided route with the currently selected tab
 * @param {Route} route the route instance to update
 * @param {Tabs} tabSelected identifier for the selected tab
 * @returns {Tabs}
 */
const setTabSelectedOnAncestorController = (route: Route, tabSelected: Tabs): Tabs => {
  const { routeName, controllerFor } = route;
  assert('route should be a descendant of datasets.dataset', !routeName.indexOf('datasets.dataset.'));
  const ancestorController = <DatasetController>controllerFor.call(route, 'datasets.dataset');

  return set(ancestorController, 'tabSelected', tabSelected);
};

/**
 * Factory creates a dataset Route class that sets the currently selected tab on the parent controller
 * @param {{ selectedTab: Tabs }} { selectedTab } options bag contains identifier for the current tab
 * @returns {typeof Route} the descendant route class
 */
const descendantDatasetRouteClassFactory = ({ selectedTab }: { selectedTab: Tabs }): typeof Route => {
  return class DatasetDescendantRoute extends Route {
    actions = {
      didTransition(this: DatasetDescendantRoute) {
        // on successful route transition
        setTabSelectedOnAncestorController(this, selectedTab);
      }
    };
  };
};

export { Tabs, descendantDatasetRouteClassFactory };
