import Route from '@ember/routing/route';
import Notifications from '@datahub/utils/services/notifications';
import { refreshModelForQueryParams } from '@datahub/utils/routes/refresh-model-for-query-params';
import { inject as service } from '@ember/service';
import Search from '@datahub/shared/services/search';
import { getDatasetUrnParts } from '@datahub/data-models/entity/dataset/utils/urn';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';
/**
 * Describes the interface for properties passed into the routes's model hook
 * @interface IDatasetRouteParams
 */
interface IDatasetRouteParams {
  dataset_urn: string;
}

/**
 * Describes the return type of the model of the dataset route
 */
export interface IDatasetRouteModel {
  datasetUrn: string;
  platform?: DatasetPlatform;
}

export default class DatasetRoute extends Route {
  /**
   * References the application's Notifications service
   * @type {ComputedProperty<Notifications>}
   * @memberof DatasetRoute
   */
  @service
  notifications: Notifications;

  @service
  search: Search;

  queryParams = refreshModelForQueryParams(['urn']);

  /**
   * Resolves an IDatasetEntity from a dataset urn
   * @param {IDatasetRouteParams} { dataset_id: identifier, urn }
   * @returns {Promise<IDatasetEntity>}
   * @memberof DatasetRoute
   */
  model({ dataset_urn: datasetUrn }: IDatasetRouteParams): IDatasetRouteModel {
    // platform is needed for tracking
    const { platform } = getDatasetUrnParts(datasetUrn);
    return {
      platform,
      datasetUrn
    };
  }
}
