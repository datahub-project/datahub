import Ember from 'ember';

export default Ember.Route.extend({
  setupController(controller, model) {
    currentTab = 'Datasets';
    updateActiveTab();
    var id = 0;
    var source = '';
    var urn = '';
    var name = '';
    let instanceUrl;
    let propertiesUrl;
    const ownerTypeUrl = '/api/v1/owner/types';


    /**
     * Builds a privacyCompliancePolicy map with default / unset values for non null properties
     */
    const createPrivacyCompliancePolicy = () => {
      // TODO: Move to a more accessible location, this app does not use modules at the moment, so potentially
      // ->TODO: registering it as factory
      const complianceTypes = ['AUTO_PURGE', 'CUSTOM_PURGE', 'LIMITED_RETENTION', 'PURGE_NOT_APPLICABLE'];
      const policy = {
        //default to first item in compliance types list
        complianceType: complianceTypes.get('firstObject'),
        compliancePurgeEntities: []
      };

      // Ensure we deep clone map to prevent mutation from consumers
      return JSON.parse(JSON.stringify(policy));
    };

    /**
     * Builds a securitySpecification map with default / unset values for non null properties as per avro schema
     * @param {number} id
     */
    const createSecuritySpecification = id => {
      const classification = [
        'highlyConfidential', 'confidential', 'limitedDistribution', 'mustBeEncrypted', 'mustBeMasked'
      ].reduce((classification, classifier) => {
        classification[classifier] = [];
        return classification;
      }, {});
      const securitySpecification = {
        classification,
        datasetId: id,
        geographicAffinity: {affinity: ''},
        recordOwnerType: '',
        retentionPolicy: {retentionType: ''}
      };

      return JSON.parse(JSON.stringify(securitySpecification));
    };

    /**
     * Series of chain-able functions invoked to set set properties on the controller required for dataset tab sections
     * @type {{privacyCompliancePolicy: ((id)), securitySpecification: ((id)), datasetSchemaFieldNamesAndTypes: ((id))}}
     */
    const fetchThenSetOnController = {
      privacyCompliancePolicy(id) {
        Ember.$.getJSON(`/api/v1/datasets/${id}/compliance`)
            .then(({privacyCompliancePolicy = createPrivacyCompliancePolicy(), return_code}) =>
                controller.setProperties({
                  privacyCompliancePolicy,
                  'isNewPrivacyCompliancePolicy': return_code === 404
                }));

        return this;
      },

      securitySpecification(id) {
        Ember.$.getJSON(`/api/v1/datasets/${id}/security`)
            .then(({securitySpecification = createSecuritySpecification(id), return_code}) =>
                controller.setProperties({
                  securitySpecification,
                  'isNewSecuritySpecification': return_code === 404
                }));

        return this;
      },

      datasetSchemaFieldNamesAndTypes(id) {
        Ember.$.getJSON(`/api/v1/datasets/${id}`)
            .then(({dataset: {schema} = {schema: undefined}} = {}) => {
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
                function getFieldTypeMappingArray({schema: {fields: nestedFields = []} = {schema: {}}, fields}) {
                  fields = Array.isArray(fields) ? fields : nestedFields;

                  // As above, field may contain a label with string property or a name property
                  return fields.map(({label: {string} = {}, name, type}) => ({
                    name: string || name,
                    type: Array.isArray(type) ? (type[0] === 'null' ? type.slice(1) : type) : [type]
                  }));
                }

                schema = JSON.parse(schema);
                return [].concat(...getFieldTypeMappingArray(schema));
              }

              controller.set('datasetSchemaFieldsAndTypes', getFieldNamesAndTypesFrom(schema));
            });

        return this;
      }
    };

    controller.set("hasProperty", false);

    if (model && model.id) {
      ({id, source, urn, name} = model);
      let {originalSchema = null} = model;

      this.controllerFor('datasets').set('detailview', true);
      if (originalSchema) {
        Ember.set(model, 'schema', originalSchema);
      }

      controller.set('model', model);
    } else if (model.dataset) {
      ({id, source, urn, name} = model.dataset);

      controller.set('model', model.dataset);
      this.controllerFor('datasets').set('detailview', true);
    }

    // Don't set default zero Ids on controller
    if (id) {
      controller.set('datasetId', id);
      // Creates list of partially applied functions from `fetchThenSetController` and invokes each in turn
      Object.keys(fetchThenSetOnController)
          .map(funcRef => fetchThenSetOnController[funcRef]['bind'](fetchThenSetOnController, id))
          .forEach(func => func());
    }

    instanceUrl = '/api/v1/datasets/' + id + '/instances';
    $.get(instanceUrl, function (data) {
      if (data && data.status == "ok" && data.instances && data.instances.length > 0) {
        controller.set("hasinstances", true);
        controller.set("instances", data.instances);
        controller.set("currentInstance", data.instances[0]);
        controller.set("latestInstance", data.instances[0]);
        var versionUrl = '/api/v1/datasets/' + id + "/versions/db/" + data.instances[0].dbId;
        $.get(versionUrl, function (data) {
          if (data && data.status == "ok" && data.versions && data.versions.length > 0) {
            controller.set("hasversions", true);
            controller.set("versions", data.versions);
            controller.set("currentVersion", data.versions[0]);
            controller.set("latestVersion", data.versions[0]);
          }
          else {
            controller.set("hasversions", false);
            controller.set("currentVersion", '0');
            controller.set("latestVersion", '0');
          }
        });
      }
      else {
        controller.set("hasinstances", false);
        controller.set("currentInstance", '0');
        controller.set("latestInstance", '0');
        controller.set("hasversions", false);
        controller.set("currentVersion", '0');
        controller.set("latestVersion", '0');
      }
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

    $.get(ownerTypeUrl, function (data) {
      if (data && data.status == "ok") {
        controller.set("ownerTypes", data.ownerTypes);
      }
    });

    var userSettingsUrl = '/api/v1/user/me';
    $.get(userSettingsUrl, function (data) {
      var tabview;
      if (data && data.status == "ok") {
        controller.set("currentUser", data.user);
        if (data.user && data.user.userSetting && data.user.userSetting.detailDefaultView) {
          if (data.user.userSetting.detailDefaultView == 'tab') {
            tabview = true;
          }
        }
      }
      controller.set("tabview", true);
    });

    var columnUrl = '/api/v1/datasets/' + id + "/columns";
    $.get(columnUrl, function (data) {
      if (data && data.status == "ok") {
        if (data.columns && (data.columns.length > 0)) {
          controller.set("hasSchemas", true);

          data.columns = data.columns.map(function (item, idx) {
            if (item.comment) {
              item.commentHtml = marked(item.comment).htmlSafe()
            }
            return item
          })
          controller.set("schemas", data.columns);
          controller.buildJsonView();
          setTimeout(initializeColumnTreeGrid, 500);
        } else {
          controller.set("hasSchemas", false);
          controller.set("schemas", null);
          controller.buildJsonView();
        }
      } else {
        controller.set("hasSchemas", false);
        controller.set("schemas", null);
        controller.buildJsonView();
      }
    });

    if (source.toLowerCase() != 'pinot') {
      propertiesUrl = '/api/v1/datasets/' + id + "/properties";
      $.get(propertiesUrl, function (data) {
        if (data && data.status == "ok") {
          if (data.properties) {
            var propertyArray = convertPropertiesToArray(data.properties);
            if (propertyArray && propertyArray.length > 0) {
              controller.set("hasProperty", true);
              controller.set("properties", propertyArray);
            } else {
              controller.set("hasProperty", false);
            }
          }
        } else {
          controller.set("hasProperty", false);
        }
      });
    }

    var sampleUrl;
    if (source.toLowerCase() == 'pinot') {
      sampleUrl = '/api/v1/datasets/' + id + "/properties";
      $.get(sampleUrl, function (data) {
        if (data && data.status == "ok") {
          if (data.properties && data.properties.elements && (data.properties.elements.length > 0)
              && data.properties.elements[0] && data.properties.elements[0].columnNames
              && (data.properties.elements[0].columnNames.length > 0)) {
            controller.set("hasSamples", true);
            controller.set("samples", data.properties.elements[0].results);
            controller.set("columns", data.properties.elements[0].columnNames);
          } else {
            controller.set("hasSamples", false);
          }
        } else {
          controller.set("hasSamples", false);
        }
      });
    }
    else {
      sampleUrl = '/api/v1/datasets/' + id + "/sample";
      $.get(sampleUrl, function (data) {
        if (data && data.status == "ok") {
          if (data.sampleData && data.sampleData.sample && (data.sampleData.sample.length > 0)) {
            controller.set("hasSamples", true);
            var tmp = {};
            var count = data.sampleData.sample.length
            var d = data.sampleData.sample
            for (var i = 0; i < count; i++) {
              tmp['record ' + i] = d[i]
            }
            var node = JsonHuman.format(tmp)
            setTimeout(function () {
              var json_human = document.getElementById('datasetSampleData-json-human');
              if (json_human) {
                if (json_human.children && json_human.children.length > 0) {
                  json_human.removeChild(json_human.childNodes[json_human.children.length - 1]);
                }

                json_human.appendChild(node)
              }
            }, 500);
          } else {
            controller.set("hasSamples", false);
          }
        } else {
          controller.set("hasSamples", false);
        }
      });
    }

    var impactAnalysisUrl = '/api/v1/datasets/' + id + "/impacts";
    $.get(impactAnalysisUrl, function (data) {
      if (data && data.status == "ok") {
        if (data.impacts && (data.impacts.length > 0)) {
          controller.set("hasImpacts", true);
          controller.set("impacts", data.impacts);
        } else {
          controller.set("hasImpacts", false);
        }
      }
    });

    var datasetDependsUrl = '/api/v1/datasets/' + id + "/depends";
    $.get(datasetDependsUrl, function (data) {
      if (data && data.status == "ok") {
        if (data.depends && (data.depends.length > 0)) {
          controller.set("hasDepends", true);
          controller.set("depends", data.depends);
          setTimeout(initializeDependsTreeGrid, 500);
        } else {
          controller.set("hasDepends", false);
        }
      }
    });

    var datasetPartitionsUrl = '/api/v1/datasets/' + id + "/access";
    $.get(datasetPartitionsUrl, function (data) {
      if (data && data.status == "ok") {
        if (data.access && (data.access.length > 0)) {
          controller.set("hasAccess", true);
          controller.set("accessibilities", data.access);
        } else {
          controller.set("hasAccess", false);
        }
      }
    });

    var datasetReferencesUrl = '/api/v1/datasets/' + id + "/references";
    $.get(datasetReferencesUrl, function (data) {
      if (data && data.status == "ok") {
        if (data.references && (data.references.length > 0)) {
          controller.set("hasReferences", true);
          controller.set("references", data.references);
          setTimeout(initializeReferencesTreeGrid, 500);
        } else {
          controller.set("hasReferences", false);
        }
      }
    });

    Promise.resolve($.get(`/api/v1/datasets/${id}/owners`))
        .then(({status, owners = []}) => {
          status === 'ok' && controller.set('owners', owners);
        })
        .then($.get.bind($, '/api/v1/party/entities'))
        .then(({status, userEntities = []}) => {
          if (status === 'ok' && userEntities.length) {
            /**
             * @type {Object} userEntitiesMaps hash of userEntities: label -> displayName
             */
            const userEntitiesMaps = userEntities.reduce((map, {label, displayName}) =>
                (map[label] = displayName, map), {});

            controller.setProperties({
              userEntitiesSource: Object.keys(userEntitiesMaps),
              userEntitiesMaps
            });
          }
        });
  },

  model: ({dataset_id}) => Ember.$.getJSON(`/api/v1/datasets/${dataset_id}`),

  actions: {
    getSchema: function () {
      var controller = this.get('controller')
      var id = this.get('controller.model.id')
      var columnUrl = '/api/v1/datasets/' + id + "/columns";
      controller.set("isTable", true);
      controller.set("isJSON", false);
      $.get(columnUrl, function (data) {
        if (data && data.status == "ok") {
          if (data.columns && (data.columns.length > 0)) {
            controller.set("hasSchemas", true);
            data.columns = data.columns.map(function (item, idx) {
              item.commentHtml = marked(item.comment).htmlSafe()
              return item
            })
            controller.set("schemas", data.columns);
            setTimeout(initializeColumnTreeGrid, 500);
          }
          else {
            controller.set("hasSchemas", false);
          }
        }
        else {
          controller.set("hasSchemas", false);
        }
      });
    },
    getDataset: function () {
      $.get('/api/v1/datasets/' + this.get('controller.model.id'), data => {
        if (data.status == "ok") {
          this.set('controller.model', data.dataset)
        }
      })
    }
  }
});
