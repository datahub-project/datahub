import Route from '@ember/routing/route';
import { set, get, setProperties } from '@ember/object';
import { inject } from '@ember/service';
import { datasetIdToUrn, readDatasetByUrn } from 'wherehows-web/utils/api/datasets/dataset';
import isUrn, { isWhUrn, isLiUrn, convertWhUrnToLiUrn, encodeUrn, decodeUrn } from 'wherehows-web/utils/validators/urn';
import { refreshModelQueryParams } from 'wherehows-web/utils/helpers/routes';

export default Route.extend({
  /**
   * Runtime application configuration options
   * @type {Ember.Service}
   */
  configurator: inject(),
  /**
   * Reference to the application notifications Service
   * @type {ComputedProperty<Notifications>}
   */
  notifications: inject(),

  queryParams: refreshModelQueryParams(['urn']),

  /**
   * Reads the dataset given a identifier from the dataset endpoint
   * @param {string} dataset_id a identifier / id for the dataset to be fetched
   * @param {string} [urn] optional urn identifier for dataset
   * @return {Promise<IDataset|IDatasetView>}
   */
  async model({ dataset_id: identifier, urn }) {
    const isIdentifierUrn = isUrn(decodeUrn(String(identifier)));

    if (identifier === 'urn' || isIdentifierUrn) {
      isIdentifierUrn && (urn = identifier);
      const decodedUrn = decodeUrn(urn);

      isWhUrn(decodedUrn) && (urn = convertWhUrnToLiUrn(decodedUrn));

      if (isLiUrn(decodeUrn(urn))) {
        return await readDatasetByUrn(urn);
      }

      get(this, 'notifications.notify')('error', {
        content: 'Could not adequately determine the URN for the requested dataset.'
      });
    }

    identifier = await datasetIdToUrn(identifier);

    // recurse with dataset urn from id
    if (identifier) {
      return this.model({ dataset_id: identifier });
    }

    throw new TypeError(`Could not parse identifier ${identifier}. Please ensure format is valid.`);
  },

  serialize({ uri }) {
    // updates routes dataset_id param with dataset urn (urn property)
    return { dataset_id: uri };
  },

  afterModel(model, transition) {
    const { dataset_id } = transition.params['datasets.dataset'];

    // Check is dataset_id is a number, and replace with urn
    // urn's are the primary means of referencing a dataset
    if (!isNaN(parseInt(dataset_id, 10)) && isFinite(dataset_id)) {
      this.replaceWith('datasets.dataset', model);
    }
  },

  /**
   * resetting the urn query param when the hook is invoked
   * @param {Controller} controller
   */
  resetController(controller) {
    set(controller, 'urn', void 0);
  },

  async setupController(controller, model) {
    set(controller, 'model', model);
    setProperties(controller, {
      isInternal: await get(this, 'configurator').getConfig('isInternal')
    });
  }
});
