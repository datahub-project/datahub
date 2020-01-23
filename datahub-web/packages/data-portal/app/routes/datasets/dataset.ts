import Route from '@ember/routing/route';
import { set } from '@ember/object';
import Notifications from '@datahub/utils/services/notifications';
import { refreshModelForQueryParams } from '@datahub/utils/routes/refresh-model-for-query-params';
import { convertWhUrnToLiUrn, isLiUrn, isWhUrn } from '@datahub/data-models/entity/dataset/utils/urn';
import { readDatasetByUrn } from 'wherehows-web/utils/api/datasets/dataset';
import { IDatasetView } from 'wherehows-web/typings/api/datasets/dataset';
import { inject as service } from '@ember/service';
import Search from 'wherehows-web/services/search';
import { isNotFoundApiError } from 'wherehows-web/utils/api';
import { decodeUrn, encodeWildcard } from '@datahub/utils/validators/urn';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';

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
  dataset?: IDatasetView;
  isNotFoundApiError?: boolean;
  error?: Error;
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
   * Will read a dataset using a urn or a WH urn
   * @param {string} urn
   */
  async readDataset(urn: string): Promise<IDatasetView> {
    const decodedResolvedUrn = decodeUrn(urn);

    if (isWhUrn(decodedResolvedUrn)) {
      urn = convertWhUrnToLiUrn(decodedResolvedUrn);
    }

    if (isLiUrn(decodeUrn(urn))) {
      return await readDatasetByUrn(encodeWildcard(urn));
    }

    throw new TypeError(`Could not adequately determine the URN for the requested dataset.`);
  }

  /**
   * Will set the entity dropdown search to the right entity
   * when we read the dataset from backend
   * @param dataset
   */
  setSearchEntity(_dataset: IDatasetView): void {
    set(this.search, 'entity', DatasetEntity.displayName);
  }

  /**
   * Resolves an IDatasetView from a dataset urn
   * @param {IDatasetRouteParams} { dataset_id: identifier, urn }
   * @returns {Promise<IDatasetView>}
   * @memberof DatasetRoute
   */
  // eslint-disable-next-line @typescript-eslint/camelcase
  async model(this: DatasetRoute, { dataset_urn }: IDatasetRouteParams): Promise<IDatasetRouteModel> {
    try {
      const dataset: IDatasetView = await this.readDataset(dataset_urn);
      this.setSearchEntity(dataset);
      return { dataset };
    } catch (e) {
      return {
        isNotFoundApiError: isNotFoundApiError(e),
        error: e
      };
    }
  }
}
