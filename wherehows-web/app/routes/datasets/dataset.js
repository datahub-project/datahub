import Ember from 'ember';
import { makeUrnBreadcrumbs } from 'wherehows-web/utils/entities';
import { datasetComplianceFor, datasetComplianceSuggestionsFor } from 'wherehows-web/utils/api/datasets/compliance';
import { datasetCommentsFor } from 'wherehows-web/utils/api/datasets/comments';
import {
  getDatasetOwners,
  getUserEntities,
  isRequiredMinOwnersNotConfirmed
} from 'wherehows-web/utils/api/datasets/owners';

const { Route, get, set, setProperties, isPresent, inject: { service }, $: { getJSON } } = Ember;
// TODO: DSS-6581 Create URL retrieval module
const datasetsUrlRoot = '/api/v1/datasets';
const datasetUrl = id => `${datasetsUrlRoot}/${id}`;
const ownerTypeUrlRoot = '/api/v1/owner/types';
const getDatasetColumnUrl = id => `${datasetUrl(id)}/columns`;
const getDatasetPropertiesUrl = id => `${datasetUrl(id)}/properties`;
const getDatasetSampleUrl = id => `${datasetUrl(id)}/sample`;
const getDatasetImpactAnalysisUrl = id => `${datasetUrl(id)}/impacts`;
const getDatasetDependsUrl = id => `${datasetUrl(id)}/depends`;
const getDatasetPartitionsUrl = id => `${datasetUrl(id)}/access`;
const getDatasetReferencesUrl = id => `${datasetUrl(id)}/references`;
const getDatasetInstanceUrl = id => `${datasetUrl(id)}/instances`;
const getDatasetVersionUrl = (id, dbId) => `${datasetUrl(id)}/versions/db/${dbId}`;

let getDatasetColumn;

export default Route.extend({
  /**
   * Runtime application configuration options
   * @type {Ember.Service}
   */
  configurator: service(),

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
       * Fetch the datasetColumn
       * @param {number} id the id of the dataset
       */
      getDatasetColumn = id =>
        Promise.resolve(getJSON(getDatasetColumnUrl(id)))
          .then(({ status, columns = [] }) => {
            if (status === 'ok') {
              if (columns && columns.length) {
                const columnsWithHTMLComments = columns.map(column => {
                  const { comment } = column;

                  if (comment) {
                    // TODO: DSS-6122 Refactor global function reference
                    column.commentHtml = window.marked(comment).htmlSafe();
                  }

                  return column;
                });

                controller.set('hasSchemas', true);
                controller.set('schemas', columnsWithHTMLComments);

                // TODO: DSS-6122 Refactor direct method invocation on controller
                controller.buildJsonView();
                // TODO: DSS-6122 Refactor setTimeout,
                //   global function reference
                setTimeout(window.initializeColumnTreeGrid, 500);
              }

              return columns;
            }

            return Promise.reject(new Error('Dataset columns request failed.'));
          })
          .then(columns => columns.map(({ dataType, fullFieldPath }) => ({ dataType, fieldName: fullFieldPath })))
          .catch(() => {
            setProperties(controller, { hasSchemas: false, schemas: null });
            return [];
          });

      /**
       * async IIFE sets the the complianceInfo and schemaFieldNamesMappedToDataTypes
       * at once so observers will be buffered
       * @param {number} id the dataset id
       * @return {Promise.<void>}
       */
      (async id => {
        const [columns, compliance, complianceSuggestion, datasetComments] = await Promise.all([
          getDatasetColumn(id),
          datasetComplianceFor(id),
          datasetComplianceSuggestionsFor(id),
          datasetCommentsFor(id)
        ]);
        const { complianceInfo, isNewComplianceInfo } = compliance;

        setProperties(controller, {
          complianceInfo,
          isNewComplianceInfo,
          complianceSuggestion,
          schemaFieldNamesMappedToDataTypes: columns,
          datasetComments
        });
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

  model: ({ dataset_id }) => {
    const datasetUrl = `${datasetsUrlRoot}/${dataset_id}`;

    return Promise.resolve(getJSON(datasetUrl)).then(({ status, dataset, message = '' }) => {
      return status === 'ok' && isPresent(dataset)
        ? dataset
        : Promise.reject(
            new Error(
              `Request for ${datasetUrl} failed with status: ${status}.
              ${message}`
            )
          );
    });
  },

  actions: {
    getSchema: function() {
      const controller = get(this, 'controller');
      const id = get(controller, 'model.id');

      set(controller, 'isTable', true);
      set(controller, 'isJSON', false);
      typeof getDatasetColumn === 'function' && getDatasetColumn(id);
    },

    getDataset() {
      Promise.resolve(getJSON(datasetUrl(this.get('controller.model.id')))).then(
        ({ status, dataset }) => status === 'ok' && set(this, 'controller.model', dataset)
      );
    }
  }
});
