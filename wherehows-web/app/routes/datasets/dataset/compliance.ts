import { set } from '@ember/object';
import { descendantDatasetRouteClassFactory, Tabs } from 'wherehows-web/constants/datasets/shared';
import DatasetComplianceController from 'wherehows-web/controllers/datasets/dataset/compliance';
import { refreshModelForQueryParams } from 'wherehows-web/utils/helpers/routes';
import { IDatasetView } from 'wherehows-web/typings/api/datasets/dataset';
import DatasetController from 'wherehows-web/controllers/datasets/dataset';

export default class Compliance extends descendantDatasetRouteClassFactory({ selectedTab: Tabs.Compliance }) {
  queryParams = refreshModelForQueryParams(['fieldFilter']);

  setupController(controller: DatasetComplianceController, model: IDatasetView): void {
    const { setPropOnAncestorController } = this;

    super.setupController(controller, model);

    // set the compliance tagFilter on the ancestor controller
    setPropOnAncestorController('complianceTagFilter', controller.fieldFilter);
  }

  /**
   * Sets a DatasetController property K to the supplied value
   * @template K keyof DatasetController
   * @param {K} prop the property on the DatasetController to be set
   * @param {DatasetController[K]} value value to be applied to prop
   * @returns {DatasetController[K]}
   * @memberof Compliance
   */
  setPropOnAncestorController = <K extends keyof DatasetController>(
    prop: K,
    value: DatasetController[K]
  ): DatasetController[K] => set(this.controllerFor('datasets.dataset'), prop, value);
}
