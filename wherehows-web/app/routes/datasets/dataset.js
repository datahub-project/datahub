import Ember from 'ember';
import {
  createPrivacyCompliancePolicy,
  createSecuritySpecification
} from 'wherehows-web/utils/datasets/functions';

const {
  Route,
  get,
  set,
  setProperties,
  isPresent,
  $: { getJSON }
} = Ember;
// TODO: DSS-6581 Create URL retrieval module
const datasetsUrlRoot = '/api/v1/datasets';
const datasetUrl = id => `${datasetsUrlRoot}/${id}`;
const ownerTypeUrlRoot = '/api/v1/owner/types';
const userSettingsUrlRoot = '/api/v1/user/me';
const partyEntitiesUrl = '/api/v1/party/entities';
const getDatasetColumnUrl = id => `${datasetUrl(id)}/columns`;
const getDatasetPropertiesUrl = id => `${datasetUrl(id)}/properties`;
const getDatasetSampleUrl = id => `${datasetUrl(id)}/sample`;
const getDatasetImpactAnalysisUrl = id => `${datasetUrl(id)}/impacts`;
const getDatasetDependsUrl = id => `${datasetUrl(id)}/depends`;
const getDatasetPartitionsUrl = id => `${datasetUrl(id)}/access`;
const getDatasetReferencesUrl = id => `${datasetUrl(id)}/references`;
const getDatasetOwnersUrl = id => `${datasetUrl(id)}/owners`;
const getDatasetInstanceUrl = id => `${datasetUrl(id)}/instances`;
const getDatasetVersionUrl = (id, dbId) =>
  `${datasetUrl(id)}/versions/db/${dbId}`;
const getDatasetSecurityUrl = id => `${datasetUrl(id)}/security`;
const getDatasetComplianceUrl = id => `${datasetUrl(id)}/compliance`;

let getDatasetColumn;

export default Route.extend({
  //TODO: DSS-6632 Correct server-side if status:error and record not found but response is 200OK
  setupController(controller, model) {
    currentTab = 'Datasets';
    window.updateActiveTab();
    let source = '';
    var id = 0;
    var urn = '';

    /**
     * Series of chain-able functions invoked to set set properties on the controller required for dataset tab sections
     * @type {{privacyCompliancePolicy: ((id)), securitySpecification: ((id)), datasetSchemaFieldNamesAndTypes: ((id))}}
     */
    const fetchThenSetOnController = {
      privacyCompliancePolicy(id, controller) {
        Promise.resolve(getJSON(getDatasetComplianceUrl(id))).then(({
          privacyCompliancePolicy = createPrivacyCompliancePolicy(),
          return_code
        }) =>
          setProperties(controller, {
            privacyCompliancePolicy,
            isNewPrivacyCompliancePolicy: return_code === 404
          }));

        return this;
      },

      securitySpecification(id, controller) {
        Promise.resolve(getJSON(getDatasetSecurityUrl(id))).then(({
          securitySpecification = createSecuritySpecification(id),
          return_code
        }) =>
          setProperties(controller, {
            securitySpecification,
            isNewSecuritySpecification: return_code === 404
          }));

        return this;
      },

      datasetSchemaFieldNamesAndTypes(id, controller) {
        Promise.resolve(getJSON(datasetUrl(id))).then(({
          dataset: { schema } = { schema: undefined }
        } = {}) => {
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
            function getFieldTypeMappingArray(
              { schema: { fields: nestedFields = [] } = { schema: {} }, fields }
            ) {
              fields = Array.isArray(fields) ? fields : nestedFields;

              // As above, field may contain a label with string property or a name property
              return fields.map(({ label: { string } = {}, name, type }) => ({
                name: string || name,
                type: Array.isArray(type)
                  ? type[0] === 'null' ? type.slice(1) : type
                  : [type]
              }));
            }

            schema = JSON.parse(schema);
            return [...getFieldTypeMappingArray(schema)];
          }

          set(
            controller,
            'datasetSchemaFieldsAndTypes',
            getFieldNamesAndTypesFrom(schema)
          );
        });

        return this;
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
          fetchThenSetOnController[funcRef]['bind'](
            fetchThenSetOnController,
            id,
            controller
          ))
        .forEach(func => func());
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

          return Promise.resolve(
            getJSON(getDatasetVersionUrl(id, dbId))
          ).then(({ status, versions = [] }) => {
            if (status === 'ok' && versions.length) {
              const [firstVersion] = versions;

              setProperties(controller, {
                versions,
                hasversions: true,
                currentVersion: firstVersion,
                latestVersion: firstVersion
              });
            }

            return Promise.reject(
              new Error('Dataset versions request failed.')
            );
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

    if (urn) {
      var index = urn.lastIndexOf('/');
      if (index != -1) {
        var listUrl = '/api/v1/list/datasets?urn=' + urn.substring(0, index + 1);
        $.get(listUrl, function (data) {
          if (data && data.status == "ok") {
            // renderDatasetListView(data.nodes, name);
          }
        });
      }
    }

    if (datasetCommentsComponent) {
      datasetCommentsComponent.getComments();
    }

    if (urn) {
      urn = urn.replace('<b>', '').replace('</b>', '');
      var index = urn.lastIndexOf("/");
      if (index != -1) {
        var name = urn.substring(index + 1);
        // Ember.run.scheduleOnce('afterRender', null, findAndActiveDatasetNode, name, urn);
      }
      // var breadcrumbs = [{"title": "DATASETS_ROOT", "urn": "1", destRoute: 'datasets.page'}];
      let breadcrumbs = [
        {
          route: 'datasets.page',
          text: 'datasets',
          model: 1
        }
      ];
      var updatedUrn = urn.replace("://", "");
      var b = updatedUrn.split('/');
      for (var i = 0; i < b.length; i++) {
        if (i === 0) {
          breadcrumbs.push({
            text: b[i],
            // urn: "name/" + b[i] + "/page/1?urn=" + b[i] + ':///',
            model: [b[i], 1],
            queryParams: {urn: b[i] + ':///'},
            route: 'datasets.name.subpage'
          });
        } else if (i === (b.length - 1)) {
          breadcrumbs.push({
            text: b[i],
            model: id,
            route: 'datasets.dataset'
          });
        } else {
          breadcrumbs.push({
            text: b[i],
            // urn: "name/" + b[i] + "/page/1?urn=" + urn.split('/').splice(0, i + 3).join('/'),
            model: [b[i], 1],
            queryParams: {urn: urn.split('/').splice(0, i + 3).join('/')},
            route: 'datasets.name.subpage'
          });
        }
      }
      controller.set("breadcrumbs", breadcrumbs);
    }

    Promise.resolve(getJSON(ownerTypeUrlRoot)).then(
      ({ status, ownerTypes }) =>
        status === 'ok' && set(controller, 'ownerTypes', ownerTypes)
    );

    Promise.resolve(getJSON(userSettingsUrlRoot)).then(({ status, user }) => {
      if (status === 'ok') {
        // TODO: DSS-6633 Remove `data.user.userSetting.detailDefaultView` from
        //   '/api/v1/user/me'  endpoint
        controller.set('currentUser', user);
      }
    });

    getDatasetColumn = id =>
      Promise.resolve(getJSON(getDatasetColumnUrl(id)))
        .then(({ status, columns }) => {
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

            return;
          }
          return Promise.reject(new Error('Dataset columns request failed.'));
        })
        .catch(() =>
          setProperties(controller, {
            hasSchemas: false,
            schemas: null
          }));

    getDatasetColumn(id);

    if (source.toLowerCase() !== 'pinot') {
      Promise.resolve(getJSON(getDatasetPropertiesUrl(id)))
        .then(({ status, properties }) => {
          if (status === 'ok' && properties) {
            const propertyArray = window.convertPropertiesToArray(
              properties
            ) || [];
            if (propertyArray.length) {
              controller.set('hasProperty', true);

              return controller.set('properties', propertyArray);
            }
          }

          return Promise.reject(
            new Error('Dataset properties request failed.')
          );
        })
        .catch(() => controller.set('hasProperty', false));

      Promise.resolve(getJSON(getDatasetSampleUrl(id)))
        .then(({ status, sampleData = {} }) => {
          if (status === 'ok') {
            const { sample = [] } = sampleData;
            const { length } = sample;
            if (length) {
              const samplesObject = sample.reduce(
                (sampleObject, record, index) =>
                  ((sampleObject[`record${index}`] = record), sampleObject),
                {}
              );
              // TODO: DSS-6122 Refactor global function reference
              const node = window.JsonHuman.format(samplesObject);

              controller.set('hasSamples', true);

              // TODO: DSS-6122 Refactor setTimeout
              setTimeout(
                function() {
                  const jsonHuman = document.getElementById(
                    'datasetSampleData-json-human'
                  );
                  if (jsonHuman) {
                    if (jsonHuman.children && jsonHuman.children.length) {
                      jsonHuman.removeChild(
                        jsonHuman.childNodes[jsonHuman.children.length - 1]
                      );
                    }

                    jsonHuman.appendChild(node);
                  }
                },
                500
              );
            }

            return;
          }

          return Promise.reject(new Error('Dataset sample request failed.'));
        })
        .catch(() => set(controller, 'hasSamples', false));
    }

    if (source.toLowerCase() === 'pinot') {
      Promise.resolve(getJSON(getDatasetPropertiesUrl(id))).then(({
        status,
        properties = {}
      }) => {
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

        return Promise.reject(
          new Error('Dataset impact analysis request failed.')
        );
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

    Promise.resolve(getJSON(getDatasetOwnersUrl(id)))
      .then(({ status, owners = [] }) => {
        status === 'ok' && set(controller, 'owners', owners);
      })
      .then(() => getJSON(partyEntitiesUrl))
      .then(({ status, userEntities = [] }) => {
        if (status === 'ok' && userEntities.length) {
          /**
           * @type {Object} userEntitiesMaps hash of userEntities: label -> displayName
           */
          const userEntitiesMaps = userEntities.reduce(
            (map, { label, displayName }) => ((map[label] = displayName), map),
            {}
          );

          setProperties(controller, {
            userEntitiesMaps,
            userEntitiesSource: Object.keys(userEntitiesMaps)
          });
        }
      });
  },

  model: ({ dataset_id }) => {
    const datasetUrl = `${datasetsUrlRoot}/${dataset_id}`;

    return Promise.resolve(getJSON(datasetUrl))
      .then(({ status, dataset }) => {
        return status === 'ok' && isPresent(dataset)
          ? dataset
          : Promise.reject(
              new Error(`Request for ${datasetUrl} failed with: ${status}`)
            );
      })
      .catch(() => ({}));
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
      Promise.resolve(
        getJSON(datasetUrl(this.get('controller.model.id')))
      ).then(
        ({ status, dataset }) =>
          status === 'ok' && set(this, 'controller.model', dataset)
      );
    }
  }
});
