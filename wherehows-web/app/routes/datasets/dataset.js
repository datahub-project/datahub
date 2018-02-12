import Route from '@ember/routing/route';
import { set, get, setProperties } from '@ember/object';
import { inject } from '@ember/service';
import $ from 'jquery';
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
import { readDataset, datasetUrnToId, readDatasetView } from 'wherehows-web/utils/api/datasets/dataset';
import isDatasetUrn from 'wherehows-web/utils/validators/urn';

import { checkAclAccess } from 'wherehows-web/utils/api/datasets/acl-access';
import { currentUser } from 'wherehows-web/utils/api/authentication';

const { getJSON } = $;
// TODO: DSS-6581 Move to URL retrieval module
const datasetsUrlRoot = '/api/v1/datasets';
const datasetUrl = id => `${datasetsUrlRoot}/${id}`;
const ownerTypeUrlRoot = '/api/v1/owner/types';
const getDatasetSampleUrl = id => `${datasetUrl(id)}/sample`;
const getDatasetImpactAnalysisUrl = id => `${datasetUrl(id)}/impacts`;
const getDatasetDependsUrl = id => `${datasetUrl(id)}/depends`;
const getDatasetPartitionsUrl = id => `${datasetUrl(id)}/access`;
const getDatasetReferencesUrl = id => `${datasetUrl(id)}/references`;
const getDatasetInstanceUrl = id => `${datasetUrl(id)}/instances`;
const getDatasetVersionUrl = (id, dbId) => `${datasetUrl(id)}/versions/db/${dbId}`;

export default Route.extend({
  /**
   * Runtime application configuration options
   * @type {Ember.Service}
   */
  configurator: inject(),

  queryParams: {
    urn: {
      refreshModel: true
    }
  },

  /**
   * Reads the dataset given a identifier from the dataset endpoint
   * @param {string} dataset_id a identifier / id for the dataset to be fetched
   * @param {string} [urn] optional urn identifier for dataset
   * @return {Promise<IDataset>}
   */
  async model({ dataset_id, urn }) {
    let datasetId = dataset_id;

    if (datasetId === 'urn' && isDatasetUrn(urn)) {
      datasetId = await datasetUrnToId(urn);
    }

    return await readDataset(datasetId);
  },

  /**
   * resetting the urn query param when the hook is invoked
   * @param {Controller} controller
   */
  resetController(controller) {
    set(controller, 'urn', void 0);
  },

  //TODO: DSS-6632 Correct server-side if status:error and record not found but response is 200OK
  setupController(controller, model) {
    let source = '';
    let id = 0;
    let urn = '';

    if (model && model.id) {
      ({ id, source, urn } = model);
      let { originalSchema = null } = model;

      this.controllerFor('datasets').set('detailview', true);
      if (originalSchema) {
        set(model, 'schema', originalSchema);
      }

      controller.set('model', model);
    } else if (model.dataset) {
      ({ id, source, urn } = model.dataset);

      controller.set('model', model.dataset);
      this.controllerFor('datasets').set('detailview', true);
    }

    // Don't set default zero Ids on controller
    if (id) {
      id = +id;
      controller.set('datasetId', id);

      /**
       * ****************************
       * Note: Refactor in progress *
       * ****************************
       * async IIFE sets the the complianceInfo and schemaFieldNamesMappedToDataTypes
       * at once so observers will be buffered
       * @param {number} id the dataset id
       * @return {Promise.<void>}
       */
      (async id => {
        try {
          let properties;

          const [
            { schemaless, columns },
            compliance,
            complianceDataTypes,
            complianceSuggestion,
            datasetComments,
            isInternal,
            datasetView,
            owners,
            { userEntitiesSource, userEntitiesMaps }
          ] = await Promise.all([
            readDatasetColumns(id),
            readDatasetCompliance(id),
            readComplianceDataTypes(),
            readDatasetComplianceSuggestion(id),
            readDatasetComments(id),
            get(this, 'configurator').getConfig('isInternal'),
            readDatasetView(id),
            readDatasetOwners(id),
            getUserEntities()
          ]);
          const { complianceInfo, isNewComplianceInfo } = compliance;
          const schemas = augmentObjectsWithHtmlComments(columns);

          if (String(source).toLowerCase() === 'pinot') {
            properties = await readPinotProperties(id);
          } else {
            properties = { properties: await readNonPinotProperties(id) };
          }

          setProperties(controller, {
            complianceInfo,
            complianceDataTypes,
            isNewComplianceInfo,
            complianceSuggestion,
            datasetComments,
            schemaless,
            schemas,
            isInternal,
            datasetView,
            schemaFieldNamesMappedToDataTypes: columnDataTypesAndFieldNames(columns),
            ...properties,
            owners,
            userEntitiesMaps,
            userEntitiesSource,
            requiredMinNotConfirmed: isRequiredMinOwnersNotConfirmed(owners)
          });
        } catch (e) {
          throw e;
        }
      })(id);
    }

    Promise.resolve(getJSON(getDatasetInstanceUrl(id)))
      .then(({ status, instances = [] }) => {
        if (status === 'ok' && instances.length) {
          const [firstInstance = {}] = instances;
          const { dbId } = firstInstance;

          setProperties(controller, {
            instances,
            hasinstances: true,
            currentInstance: firstInstance,
            latestInstance: firstInstance
          });

          return Promise.resolve(getJSON(getDatasetVersionUrl(id, dbId))).then(({ status, versions = [] }) => {
            if (status === 'ok' && versions.length) {
              const [firstVersion] = versions;

              setProperties(controller, {
                versions,
                hasversions: true,
                currentVersion: firstVersion,
                latestVersion: firstVersion
              });
            }

            return Promise.reject(new Error('Dataset versions request failed.'));
          });
        }

        return Promise.reject(new Error('Dataset instances request failed.'));
      })
      .catch(() => {
        setProperties(controller, {
          hasinstances: false,
          hasversions: false,
          currentInstance: '0',
          latestInstance: '0',
          currentVersion: '0',
          latestVersion: '0'
        });
      });

    // If urn exists, create a breadcrumb list
    // TODO: DSS-7068 Refactoring in progress , move this to a computed prop on a container component
    // FIXME: DSS-7068 browse.entity?urn route does not exist for last item in breadcrumb i.e. the dataset
    //  currently being viewed. Should this even be a link in the first place?
    if (urn) {
      set(controller, 'breadcrumbs', makeUrnBreadcrumbs(urn));
    }

    // Get the list of ownerTypes from endpoint,
    //   then prevent display of the `consumer`
    //   insert on controller
    Promise.resolve(getJSON(ownerTypeUrlRoot)).then(({ status, ownerTypes = [] }) => {
      ownerTypes = ownerTypes
        .filter(ownerType => String(ownerType).toLowerCase() !== 'consumer')
        .sort((a, b) => a.localeCompare(b));

      status === 'ok' && set(controller, 'ownerTypes', ownerTypes);
    });

    if (source.toLowerCase() !== 'pinot') {
      Promise.resolve(getJSON(getDatasetSampleUrl(id)))
        .then(({ status, sampleData = {} }) => {
          if (status === 'ok') {
            const { sample = [] } = sampleData;
            const { length } = sample;
            if (length) {
              const samplesObject = sample.reduce(
                (sampleObject, record, index) => ((sampleObject[`record${index}`] = record), sampleObject),
                {}
              );
              // TODO: DSS-6122 Refactor global function reference
              const node = window.JsonHuman.format(samplesObject);

              controller.set('hasSamples', true);

              // TODO: DSS-6122 Refactor setTimeout
              setTimeout(function() {
                const jsonHuman = document.getElementById('datasetSampleData-json-human');
                if (jsonHuman) {
                  if (jsonHuman.children && jsonHuman.children.length) {
                    jsonHuman.removeChild(jsonHuman.childNodes[jsonHuman.children.length - 1]);
                  }

                  jsonHuman.appendChild(node);
                }
              }, 500);
            }

            return;
          }

          return Promise.reject(new Error('Dataset sample request failed.'));
        })
        .catch(() => set(controller, 'hasSamples', false));
    }

    Promise.resolve(getJSON(getDatasetImpactAnalysisUrl(id)))
      .then(({ status, impacts = [] }) => {
        if (status === 'ok') {
          if (impacts.length) {
            return setProperties(controller, {
              hasImpacts: true,
              impacts: impacts
            });
          }
        }

        return Promise.reject(new Error('Dataset impact analysis request failed.'));
      })
      .catch(() => set(controller, 'hasImpacts', false));

    Promise.resolve(getJSON(getDatasetDependsUrl(id)))
      .then(({ status, depends = [] }) => {
        if (status === 'ok') {
          if (depends.length) {
            // TODO: DSS-6122 Refactor setTimeout,
            //   global function reference
            setTimeout(window.initializeDependsTreeGrid, 500);
            return setProperties(controller, {
              depends,
              hasDepends: true
            });
          }
        }

        return Promise.reject(new Error('Dataset depends request failed.'));
      })
      .catch(() => set(controller, 'hasDepends', false));

    Promise.resolve(getJSON(getDatasetPartitionsUrl(id)))
      .then(({ status, access = [] }) => {
        if (status === 'ok' && access.length) {
          return setProperties(controller, {
            hasAccess: true,
            accessibilities: access
          });
        }

        return Promise.reject(new Error('Dataset partitions request failed.'));
      })
      .catch(() => set(controller, 'hasAccess', false));

    Promise.resolve(getJSON(getDatasetReferencesUrl(id)))
      .then(({ status, references = [] }) => {
        if (status === 'ok' && references.length) {
          setTimeout(window.initializeReferencesTreeGrid, 500);

          return setProperties(controller, {
            references,
            hasReferences: true
          });
        }

        return Promise.reject(new Error('Dataset references request failed.'));
      })
      .catch(() => set(controller, 'hasReferences', false));

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
  },

  actions: {
    getDataset() {
      Promise.resolve(getJSON(datasetUrl(this.get('controller.model.id')))).then(
        ({ status, dataset }) => status === 'ok' && set(this, 'controller.model', dataset)
      );
    }
  }
});
