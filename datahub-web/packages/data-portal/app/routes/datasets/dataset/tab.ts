import Route from '@ember/routing/route';
import { refreshModelForQueryParams } from '@datahub/utils/routes/refresh-model-for-query-params';
import { IDatasetRouteModel } from 'wherehows-web/routes/datasets/dataset';
import { IDatasetView } from 'wherehows-web/typings/api/datasets/dataset';
import DatasetTab from 'wherehows-web/controllers/datasets/dataset/tab';
import { set } from '@ember/object';

/**
 * Describes the params received by this route
 */
interface IDatasetTabParams {
  tab_selected: string;
  field_filter: string;
  metric_urn: string;
}

/**
 * Describes the interface the return value of DatasetTabRoute
 * @interface IDatasetTabRouteModelReturnType
 */
interface IDatasetTabRouteModelReturnType {
  dataset: IDatasetView;
  currentTab: string;
  complianceTagFilter: string;
  metricUrn: string;
}

/**
 * Route that will pass route params to container component
 */
export default class DatasetTabRoute extends Route {
  queryParams = refreshModelForQueryParams(['field_filter', 'metric_urn']);

  /**
   * Implements the model hook on DatasetTabRoute inherited from Ember Route class
   * @param {IDatasetTabParams} { tab_selected, field_filter, metric_urn }
   * @returns {IDatasetTabRouteModelReturnType}
   * @memberof DatasetTabRoute
   */
  // eslint-disable-next-line @typescript-eslint/camelcase
  model({ tab_selected, field_filter, metric_urn }: IDatasetTabParams): IDatasetTabRouteModelReturnType {
    const { dataset } = this.modelFor('datasets.dataset') as IDatasetRouteModel;

    if (!dataset) {
      throw new Error('Dataset must be defined in tab route');
    }

    return {
      dataset,
      currentTab: tab_selected, // eslint-disable-line @typescript-eslint/camelcase
      complianceTagFilter: field_filter, // eslint-disable-line @typescript-eslint/camelcase
      metricUrn: metric_urn // eslint-disable-line @typescript-eslint/camelcase
    };
  }

  resetController(controller: DatasetTab, isExiting: boolean): void {
    if (isExiting) {
      set(controller, 'metric_urn', undefined);
    }
  }
}
