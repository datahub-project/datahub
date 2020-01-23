import Route from '@ember/routing/route';
import { IDatasetRouteModel } from 'wherehows-web/routes/datasets/dataset';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';

export default class DatasetIndexRoute extends Route {
  model(): void {
    const { dataset } = this.modelFor('datasets.dataset') as IDatasetRouteModel;

    // if no dataset was read then, do nothing
    if (!dataset) {
      return;
    }

    this.transitionTo('datasets.dataset.tab', DatasetEntity.renderProps.entityPage.defaultTab);
  }
}
