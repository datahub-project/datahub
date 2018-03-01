import Route from '@ember/routing/route';
import { set, get, setProperties } from '@ember/object';
import { inject } from '@ember/service';
import { makeUrnBreadcrumbs } from 'wherehows-web/utils/entities';
import { isRequiredMinOwnersNotConfirmed } from 'wherehows-web/constants/datasets/owner';
import { datasetIdToUrn, readDatasetByUrn } from 'wherehows-web/utils/api/datasets/dataset';
import isUrn, { isWhUrn, isLiUrn, convertWhUrnToLiUrn, encodeUrn, decodeUrn } from 'wherehows-web/utils/validators/urn';

import { checkAclAccess } from 'wherehows-web/utils/api/datasets/acl-access';
import { currentUser } from 'wherehows-web/utils/api/authentication';
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

    // recurse with dataset urn from id
    return this.model({ dataset_id: await datasetIdToUrn(identifier) });
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

    // TODO: Get current user ACL permission info for ACL access tab
    Promise.resolve(currentUser())
      .then(userInfo => {
        setProperties(controller, {
          userInfo
        });
        return checkAclAccess(userInfo.userName).then(value => {
          setProperties(controller, {
            aclAccessResponse: value,
            currentUserInfo: userInfo.userName,
            aclUsers: value.body
          });
        });
      })
      .catch(error => {
        setProperties(controller, {
          aclAccessResponse: null,
          currentUserInfo: ''
        });
      });
  }
});
