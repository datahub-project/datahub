import {
  descendantDatasetRouteClassFactory,
  setPropertyOnAncestorController,
  Tabs
} from 'wherehows-web/constants/datasets/shared';
import DatasetComplianceController from 'wherehows-web/controllers/datasets/dataset/compliance';
import { refreshModelForQueryParams } from 'wherehows-web/utils/helpers/routes';
import { isTagFilter, TagFilter } from 'wherehows-web/constants';
import DatasetController from 'wherehows-web/controllers/datasets/dataset';

export default class Compliance extends descendantDatasetRouteClassFactory({ selectedTab: Tabs.Compliance }) {
  queryParams = refreshModelForQueryParams(['fieldFilter']);

  async model(): Promise<DatasetController['model'] | object> {
    return { ...this.controllerFor('datasets.dataset').model };
  }

  setupController(controller: DatasetComplianceController, model: object): void {
    const { fieldFilter } = controller;

    super.setupController(controller, model);

    // set the complianceTagFilter property on the ancestor controller
    setPropertyOnAncestorController(
      this,
      'complianceTagFilter',
      isTagFilter(fieldFilter) ? fieldFilter : TagFilter.showAll
    );
  }
}
