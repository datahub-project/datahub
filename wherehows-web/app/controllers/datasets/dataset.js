import Ember from 'ember';

const {
  get,
  getWithDefault,
  $: {post}
} = Ember;

// TODO: DSS-6581 Create URL retrieval module
const datasetsUrlRoot = '/api/v1/datasets';
const datasetUrl = id => `${datasetsUrlRoot}/${id}`;
const getDatasetOwnersUrl = id => `${datasetUrl(id)}/owners`;


export default Ember.Controller.extend({
  hasProperty: false,
  hasImpacts: false,
  hasSchemas: false,
  hasSamples: false,
  isTable: true,
  isJSON: false,
  currentVersion: '0',
  latestVersion: '0',
  ownerTypes: [],
  datasetSchemaFieldsAndTypes: [],
  userTypes: [{name: "Corporate User", value: "urn:li:corpuser"}, {name: "Group User", value: "urn:li:griduser"}],
  isPinot: function () {
    var model = this.get("model");
    if (model) {
      if (model.source) {
        return model.source.toLowerCase() == 'pinot';
      }
    }
    return false;

  }.property('model.source'),
  isHDFS: function () {
    var model = this.get("model");
    if (model) {
      if (model.urn) {
        return model.urn.substring(0, 7) == 'hdfs://';
      }
    }
    return false;

  }.property('model.urn'),
  isSFDC: function () {
    var model = this.get("model");
    if (model) {
      if (model.source) {
        return model.source.toLowerCase() == 'salesforce';
      }
    }
    return false;

  }.property('model.source'),
  lineageUrl: function () {
    var model = this.get("model");
    if (model) {
      if (model.id) {
        return '/lineage/dataset/' + model.id;
      }
    }
    return '';

  }.property('model.id'),
  schemaHistoryUrl: function () {
    var model = this.get("model");
    if (model) {
      if (model.id) {
        return '/schemaHistory#/schemas/' + model.id;
      }
    }
    return '';
  }.property('model.id'),
  adjustPanes: function () {
    var hasProperty = this.get('hasProperty')
    var isHDFS = this.get('isHDFS')
    if (hasProperty && !isHDFS) {
      $("#sampletab").css('overflow', 'scroll');
      // Adjust the height
      // Set global adjuster
      var height = ($(window).height() * 0.99) - 185;
      $("#sampletab").css('height', height);
      $(window).resize(function () {
        var height = ($(window).height() * 0.99) - 185;
        $('#sampletab').height(height)
      })
    }
  }.observes('hasProperty', 'isHDFS').on('init'),
  buildJsonView: function () {
    var model = this.get("model");
    var schema = JSON.parse(model.schema)
    setTimeout(function () {
      $("#json-viewer").JSONView(schema)
    }, 500);
  },
  refreshVersions: function (dbId) {
    var model = this.get("model");
    if (!model || !model.id) {
      return;
    }
    var versionUrl = '/api/v1/datasets/' + model.id + "/versions/db/" + dbId;
    $.get(versionUrl, data => {
      if (data && data.status == "ok" && data.versions && data.versions.length > 0) {
        this.set("hasversions", true);
        this.set("versions", data.versions);
        this.set("latestVersion", data.versions[0]);
        this.changeVersion(data.versions[0]);
      }
      else {
        this.set("hasversions", false);
        this.set("currentVersion", '0');
        this.set("latestVersion", '0');
      }
    });
  },
  changeVersion: function (version) {
    _this = this;
    var currentVersion = _this.get('currentVersion');
    var latestVersion = _this.get('latestVersion');
    if (currentVersion == version) {
      return;
    }
    var objs = $('.version-btn');
    if (objs && objs.length > 0) {
      for (var i = 0; i < objs.length; i++) {
        $(objs[i]).removeClass('btn-default');
        $(objs[i]).removeClass('btn-primary');
        if (version == $(objs[i]).attr('data-value')) {
          $(objs[i]).addClass('btn-primary');
        }
        else {
          $(objs[i]).addClass('btn-default');
        }
      }
    }
    var model = this.get("model");
    if (version != latestVersion) {
      if (!model || !model.id) {
        return;
      }
      _this.set('hasSchemas', false);
      var schemaUrl = "/api/v1/datasets/" + model.id + "/schema/" + version;
      $.get(schemaUrl, function (data) {
        if (data && data.status == "ok") {
          setTimeout(function () {
            $("#json-viewer").JSONView(JSON.parse(data.schema_text))
          }, 500);
        }
      });
    }
    else {
      if (_this.schemas) {
        _this.set('hasSchemas', true);
      }
      else {
        _this.buildJsonView();
      }
    }

    _this.set('currentVersion', version);
  },

  /**
   * Given a predetermined url id, attempt to persist JSON data at url
   * @param {string} urlId
   * @param {object} data
   * @returns {Promise.<object>}
   */
  saveJson(urlId, data) {
    const postRequest = {
      type: 'POST',
      url: this.getUrlFor(urlId),
      data: JSON.stringify(data),
      contentType: 'application/json'
    };

    // If the return_code is not 200 reject the Promise
    return Promise.resolve(Ember.$.ajax(postRequest))
        .then((response = {}) => response.return_code === 200 ? response : Promise.reject(response));
  },

  getUrlFor(urlId) {
    return {
      compliance: () => `/api/v1/datasets/${this.get('datasetId')}/compliance`,
      security: () => `/api/v1/datasets/${this.get('datasetId')}/security`
    }[urlId]();
  },

  exceptionOnSave({error_message}) {
    console.error(`An error occurred on while updating : ${error_message}`);
  },

  actions: {
    /**
     * Takes the list up updated owner and posts to the server
     * @param {Ember.Array} updatedOwners the list of owner to send
     * @returns {Promise.<T>}
     */
    saveOwnerChanges(updatedOwners) {
      const datasetId = get(this, 'model.id');
      const csrfToken = getWithDefault(this, 'csrfToken', '').replace('/', '');

      return Promise.resolve(
        post({
          url: getDatasetOwnersUrl(datasetId),
          headers: {
            'Csrf-Token': csrfToken
          },
          data: {
            csrfToken,
            owners: JSON.stringify(updatedOwners)
          }
        }).then(({status = 'failed', msg = 'An error occurred.'}) => {
          if (status === 'success') {
            return {status: 'ok'};
          }

          Promise.reject({status, msg});
        })
      );
    },

    setView: function (view) {
      switch (view) {
        case "tabular":
          this.set('isTable', true);
          this.set('isJSON', false);
          $('#json-viewer').hide();
          $('#json-table').show();
          break;
        case "json":
          this.set('isTable', false);
          this.set('isJSON', true);
          this.buildJsonView();
          $('#json-table').hide();
          $('#json-viewer').show();
          break;
        default:
          this.set('isTable', true);
          this.set('isJSON', false);
          $('#json-viewer').hide();
          $('#json-table').show();
      }
    },
    updateVersion: function (version) {
      this.changeVersion(version);
    },
    updateInstance: function (instance) {
      var currentInstance = this.get('currentInstance');
      var latestInstance = this.get('latestInstance');
      if (currentInstance == instance.dbId) {
        return;
      }
      var objs = $('.instance-btn');
      if (objs && objs.length > 0) {
        for (var i = 0; i < objs.length; i++) {
          $(objs[i]).removeClass('btn-default');
          $(objs[i]).removeClass('btn-primary');

          if (instance.dbCode == $(objs[i]).attr('data-value')) {
            $(objs[i]).addClass('btn-primary');
          }
          else {
            $(objs[i]).addClass('btn-default');
          }
        }
      }

      this.set('currentInstance', instance.dbId);
      this.refreshVersions(instance.dbId);
    },

    /**
     * Requests the security specification for the current dataset id
     * and sets the result on the controller `securitySpec`
     * @returns {Promise.<*>}
     */
    resetSecuritySpecification() {
      return Ember.$.getJSON(this.getUrlFor('security'), ({return_code, securitySpecification}) => {
        return_code === 200 && this.setProperties({
          'securitySpecification': securitySpecification,
          'isNewSecuritySpecification': false
        });
      });
    },

    /**
     * Requests the privacyCompliancePolicy for the current dataset id
     * and sets the result on the controller `privacyCompliancePolicy` property
     * @returns {Promise.<*>}
     */
    resetPrivacyCompliancePolicy() {
      return Ember.$.getJSON(this.getUrlFor('compliance'), ({return_code, privacyCompliancePolicy}) => {
        return_code === 200 && this.setProperties({
          'privacyCompliancePolicy': privacyCompliancePolicy,
          'isNewPrivacyCompliancePolicy': false
        });
      });
    },

    /**
     * Retrieves the current version of the securitySpecification document and invokes an api to persist the document
     * then updates controller state if successful
     */
    saveSecuritySpecification() {
      return this.saveJson('security', this.get('securitySpecification'))
          .then(this.actions.resetSecuritySpecification.bind(this))
          .catch(this.exceptionOnSave);
    },

    /**
     * Retrieves the current version of the privacyCompliancePolicy document and invokes an api to persist the document
     * then updates controller state if successful
     */
    savePrivacyCompliancePolicy() {
      return this.saveJson('compliance', this.get('privacyCompliancePolicy'))
          .then(this.actions.resetPrivacyCompliancePolicy.bind(this))
          .catch(this.exceptionOnSave);
    }
  }
});
