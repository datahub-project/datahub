import Route from '@ember/routing/route';
import { refreshModelForQueryParams } from '@datahub/utils/routes/refresh-model-for-query-params';
import { IDatasetRouteModel } from 'datahub-web/routes/datasets/dataset';

/**
 * Describes the params received by this route
 */
interface IDatasetTabParams {
  tab_selected: string;
  field_filter: string;
  show_request_access_modal: boolean;
  request_jit_urns: Array<string>;
}

/**
 * Route that will pass route params to container component
 */
export default class DatasetTabRoute extends Route {
  queryParams = refreshModelForQueryParams(['field_filter', 'show_request_access_modal', 'request_jit_urns']);

  /**
   * Implements the model hook on DatasetTabRoute inherited from Ember Route class
   * @param {IDatasetTabParams} { tab_selected, field_filter }
   * @returns {IDatasetTabRouteModelReturnType}
   * @memberof DatasetTabRoute
   */
  redirect(params: IDatasetTabParams): void {
    const { datasetUrn } = this.modelFor('datasets.dataset') as IDatasetRouteModel;

    this.transitionTo('entity-type.urn.tab', 'datasets', datasetUrn, params.tab_selected, {
      queryParams: params
    });
  }
}
