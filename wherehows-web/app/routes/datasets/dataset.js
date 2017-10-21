import Ember from 'ember';
import { makeUrnBreadcrumbs } from 'wherehows-web/utils/entities';
import { readDatasetCompliance, readDatasetComplianceSuggestion } from 'wherehows-web/utils/api/datasets/compliance';
import { readDatasetComments } from 'wherehows-web/utils/api/datasets/comments';
import {
  readDatasetColumns,
  columnDataTypesAndFieldNames,
  columnsWithHtmlComments
} from 'wherehows-web/utils/api/datasets/columns';

import {
  getDatasetOwners,
  getUserEntities,
  isRequiredMinOwnersNotConfirmed
} from 'wherehows-web/utils/api/datasets/owners';
import { readDataset, datasetUrnToId } from 'wherehows-web/utils/api/datasets/dataset';
import isDatasetUrn from 'wherehows-web/utils/validators/urn';

const { Route, get, set, setProperties, inject: { service }, $: { getJSON }, run } = Ember;
const { schedule } = run;
// TODO: DSS-6581 Move to URL retrieval module
const datasetsUrlRoot = '/api/v1/datasets';
const datasetUrl = id => `${datasetsUrlRoot}/${id}`;
const ownerTypeUrlRoot = '/api/v1/owner/types';
const getDatasetPropertiesUrl = id => `${datasetUrl(id)}/properties`;
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
  configurator: service(),

  queryParams: {
    urn: {
      refreshModel: true
    }
  },

  /**
   * Reads the dataset given a identifier from the dataset endpoint
   * @param {string} datasetIdentifier a identifier / id for the dataset to be fetched
   * @param {string} [urn] optional urn identifier for dataset
   * @return {Promise<IDataset>}
   */
  async model({ datasetIdentifier, urn }) {
    let datasetId = datasetIdentifier;

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

    /**
     * Series of chain-able functions invoked to set set properties on the controller required for dataset tab sections
     */
    const fetchThenSetOnController = {
      datasetSchemaFieldNamesAndTypes(controller, { id }) {
        Promise.resolve(getJSON(datasetUrl(id))).then(({ dataset: { schema } = { schema: undefined } } = {}) => {
          /**
               * Parses a JSON dataset schema representation and extracts the field names and types into a list of maps
               * @param {JSON} schema
               * @returns {Array.<*>}
               */
          function getFieldNamesAndTypesFrom(schema = JSON.stringify({})) {
            /**
                 * schema argument may contain property with name `fields` or `fields` may be nested in `schema` object
                 * with same name.
                 * Unfortunately shape is inconsistent depending of dataset queried.
                 * Use `fields` property if present and is an array, otherwise use `fields` property on `schema`.
                 * Will default to empty array.
                 * @param {Array} [nestedFields]
                 * @param {Array} [fields]
                 * @returns {Array.<*>}
                 */
            function getFieldTypeMappingArray({ schema: { fields: nestedFields = [] } = { schema: {} }, fields }) {
              fields = Array.isArray(fields) ? fields : nestedFields;

              // As above, field may contain a label with string property or a name property
              return fields.map(({ label: { string } = {}, name, type }) => ({
                name: string || name,
                type: Array.isArray(type) ? (type[0] === 'null' ? type.slice(1) : type) : [type]
              }));
            }

            schema = JSON.parse(schema);
            return [...getFieldTypeMappingArray(schema)];
          }

          set(controller, 'datasetSchemaFieldsAndTypes', getFieldNamesAndTypesFrom(schema));
        });

        return this;
      },

      /**
       * Sets the isInternal flag as a property on the controller
       * @param controller {Ember.Controller} the controller to set the internal flag on
       * @param configurator {Ember.Service}
       * @return {Promise.<void>}
       */
      async isInternal(controller, { configurator }) {
        const isInternal = await configurator.getConfig('isInternal');
        set(controller, 'isInternal', isInternal);
      }
    };

    set(controller, 'hasProperty', false);

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
      controller.set('datasetId', id);
      // Creates list of partially applied functions from `fetchThenSetController` and invokes each in turn
      Object.keys(fetchThenSetOnController)
        .map(funcRef =>
          fetchThenSetOnController[funcRef]['bind'](fetchThenSetOnController, controller, {
            id,
            configurator: get(this, 'configurator')
          })
        )
        .forEach(func => func());

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
          const [columns, compliance, complianceSuggestion, datasetComments] = await Promise.all([
            readDatasetColumns(id),
            readDatasetCompliance(id),
            readDatasetComplianceSuggestion(id),
            readDatasetComments(id)
          ]);
          const { complianceInfo, isNewComplianceInfo } = compliance;
          const schemas = columnsWithHtmlComments(columns);

          setProperties(controller, {
            complianceInfo,
            isNewComplianceInfo,
            complianceSuggestion,
            datasetComments,
            schemas,
            hasSchemas: !!schemas.length,
            schemaFieldNamesMappedToDataTypes: columnDataTypesAndFieldNames(columns)
          });

          if (schemas.length) {
            run(() => {
              schedule('afterRender', null, () => {
                // TODO: DSS-6122 Refactor direct legacy method invocation on controller
                controller.buildJsonView();
                // TODO: DSS-6122 Refactor legacy global function reference
                window.initializeColumnTreeGrid();
              });
            });
          }
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
      Promise.resolve(getJSON(getDatasetPropertiesUrl(id)))
        .then(({ status, properties }) => {
          if (status === 'ok' && properties) {
            const propertyArray = window.convertPropertiesToArray(properties) || [];
            if (propertyArray.length) {
              controller.set('hasProperty', true);

              return controller.set('properties', propertyArray);
            }
          }

          return Promise.reject(new Error('Dataset properties request failed.'));
        })
        .catch(() => controller.set('hasProperty', false));

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

    if (source.toLowerCase() === 'pinot') {
      Promise.resolve(getJSON(getDatasetPropertiesUrl(id))).then(({ status, properties = {} }) => {
        if (status === 'ok') {
          const { elements = [] } = properties;
          const [{ columnNames = [], results } = {}] = elements;

          if (columnNames.length) {
            return setProperties(controller, {
              hasSamples: true,
              samples: results,
              columns: columnNames
            });
          }
        }

        return Promise.reject(new Error('Dataset properties request failed.'));
      });
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

    // Retrieve the current owners of the dataset and store on the controller
    (async id => {
      const [owners, { userEntitiesSource, userEntitiesMaps }] = await Promise.all([
        getDatasetOwners(id),
        getUserEntities()
      ]);
      setProperties(controller, {
        requiredMinNotConfirmed: isRequiredMinOwnersNotConfirmed(owners),
        owners,
        userEntitiesMaps,
        userEntitiesSource
      });
    })(id);
  },

  actions: {
    getDataset() {
      Promise.resolve(getJSON(datasetUrl(this.get('controller.model.id')))).then(
        ({ status, dataset }) => status === 'ok' && set(this, 'controller.model', dataset)
      );
    }
  }
});
