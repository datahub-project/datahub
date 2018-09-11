import Ember from 'ember'; // type import, no emit, access to Ember.Transition interface
import Route from '@ember/routing/route';
import { get, set, setProperties } from '@ember/object';
import Configurator from 'wherehows-web/services/configurator';
import Notifications, { NotificationEvent } from 'wherehows-web/services/notifications';
import { refreshModelQueryParams } from 'wherehows-web/utils/helpers/routes';
import isUrn, {
  convertWhUrnToLiUrn,
  decodeUrn,
  isLiUrn,
  isWhUrn,
  encodeWildcard
} from 'wherehows-web/utils/validators/urn';
import { datasetIdToUrn, readDatasetByUrn } from 'wherehows-web/utils/api/datasets/dataset';
import { IDatasetView } from 'wherehows-web/typings/api/datasets/dataset';
import DatasetController from 'wherehows-web/controllers/datasets/dataset';
import { service } from '@ember-decorators/service';

/**
 * Describes the interface for properties passed into the routes's model hook
 * @interface IDatasetRouteParams
 */
interface IDatasetRouteParams {
  dataset_id: string | 'urn';
  urn?: string;
}

export default class DatasetRoute extends Route {
  /**
   * References the application's Notifications service
   * @type {ComputedProperty<Notifications>}
   * @memberof DatasetRoute
   */
  @service
  notifications: Notifications;

  queryParams = refreshModelQueryParams(['urn']);

  /**
   * Resolves an IDatasetView if params can be parsed into a valid urn
   * @param {IDatasetRouteParams} { dataset_id: identifier, urn }
   * @returns {Promise<IDatasetView>}
   * @memberof DatasetRoute
   */
  async model(this: DatasetRoute, { dataset_id, urn }: IDatasetRouteParams): Promise<IDatasetView> {
    // Checks if the dataset_id matches a urn pattern
    const idIsUrn = isUrn(decodeUrn(String(dataset_id)));

    if (dataset_id === 'urn' || idIsUrn) {
      let resolvedUrn: string = idIsUrn ? dataset_id : urn!;
      const decodedResolvedUrn = decodeUrn(resolvedUrn);

      if (isWhUrn(decodedResolvedUrn)) {
        resolvedUrn = convertWhUrnToLiUrn(decodedResolvedUrn);
      }

      if (isLiUrn(decodeUrn(resolvedUrn))) {
        return await readDatasetByUrn(encodeWildcard(resolvedUrn));
      }

      get(this, 'notifications').notify(NotificationEvent.error, {
        content: 'Could not adequately determine the URN for the requested dataset.'
      });
    }

    dataset_id = await datasetIdToUrn(dataset_id);

    // recurse with dataset urn from id
    if (dataset_id) {
      return this.model({ dataset_id: dataset_id });
    }

    throw new TypeError(`Could not parse identifier ${dataset_id}. Please ensure format is valid.`);
  }

  afterModel(resolvedModel: object, transition: Ember.Transition): void {
    const { dataset_id } = transition.params['datasets.dataset'];

    // Check is dataset_id is a number, and replace with urn
    // urn's are the primary means of referencing a dataset
    if (!isNaN(parseInt(dataset_id, 10)) && isFinite(dataset_id)) {
      this.replaceWith('datasets.dataset', resolvedModel);
    }
  }

  serialize({ uri }: { uri: string }) {
    // updates routes dataset_id param with dataset urn (urn property)
    return { dataset_id: uri };
  }

  async setupController(this: DatasetRoute, controller: DatasetController, model: IDatasetView) {
    const { getConfig } = Configurator;
    set(controller, 'model', model);

    setProperties(controller, {
      isInternal: !!getConfig('isInternal'),
      jitAclAccessWhitelist: getConfig('JitAclAccessWhitelist') || [],
      jitAclContact: getConfig('jitAclContact', 'your ACL admin'),
      shouldShowDatasetLineage: getConfig('shouldShowDatasetLineage'),
      shouldShowDatasetHealth: getConfig('shouldShowDatasetHealth'),
      wikiLinks: getConfig('wikiLinks')
    });
  }

  /**
   * resetting the urn query param when the hook is invoked
   * @param {DatasetController} controller
   */
  resetController(controller: DatasetController): void {
    set(controller, 'urn', void 0);
  }
}
