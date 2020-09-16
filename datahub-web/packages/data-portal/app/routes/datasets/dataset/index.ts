import Route from '@ember/routing/route';
import { IDatasetRouteModel } from 'datahub-web/routes/datasets/dataset';

export default class DatasetIndexRoute extends Route {
  redirect(): void {
    const { datasetUrn } = this.modelFor('datasets.dataset') as IDatasetRouteModel;

    // if no dataset was read then, do nothing
    if (!datasetUrn) {
      return;
    }
    this.transitionTo('entity-type.urn', 'datasets', datasetUrn);
  }
}
