import Route from '@ember/routing/route';
import { set, get, setProperties } from '@ember/object';
import { inject } from '@ember/service';
import { makeUrnBreadcrumbs } from 'wherehows-web/utils/entities';
import { readDatasetCompliance, readDatasetComplianceSuggestion } from 'wherehows-web/utils/api/datasets/compliance';
import { readNonPinotProperties, readPinotProperties } from 'wherehows-web/utils/api/datasets/properties';
import { readDatasetComments } from 'wherehows-web/utils/api/datasets/comments';
import { readComplianceDataTypes } from 'wherehows-web/utils/api/list/compliance-datatypes';
import {
  readDatasetColumns,
  columnDataTypesAndFieldNames,
  augmentObjectsWithHtmlComments
} from 'wherehows-web/utils/api/datasets/columns';

import { readDatasetOwners, getUserEntities } from 'wherehows-web/utils/api/datasets/owners';
import { isRequiredMinOwnersNotConfirmed } from 'wherehows-web/constants/datasets/owner';
import { readDatasetById, readDatasetByUrn } from 'wherehows-web/utils/api/datasets/dataset';
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
        return await readDatasetByUrn(encodeUrn(urn));
      }

      get(this, 'notifications.notify')('error', {
        content: 'Could not adequately determine the URN for the requested dataset.'
      });
    }

    return await readDatasetById(identifier);
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
      // ...properties,
      // requiredMinNotConfirmed: isRequiredMinOwnersNotConfirmed(owners)
    });

    // If urn exists, create a breadcrumb list
    // TODO: DSS-7068 Refactoring in progress , move this to a computed prop on a container component
    // FIXME: DSS-7068 browse.entity?urn route does not exist for last item in breadcrumb i.e. the dataset
    //  currently being viewed. Should this even be a link in the first place?
    if (model.uri) {
      set(controller, 'breadcrumbs', makeUrnBreadcrumbs(model.uri));
    }

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
