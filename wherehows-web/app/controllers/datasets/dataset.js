import Controller from '@ember/controller';
import { computed, set, get, setProperties, getProperties, getWithDefault } from '@ember/object';
import { debug } from '@ember/debug';
import { inject as service } from '@ember/service';
import { run, scheduleOnce } from '@ember/runloop';
import $ from 'jquery';
import {
  datasetComplianceUrlById,
  createDatasetComment,
  readDatasetComments,
  deleteDatasetComment,
  updateDatasetComment
} from 'wherehows-web/utils/api';
import { updateDatasetDeprecation } from 'wherehows-web/utils/api/datasets/properties';
import { readDatasetView } from 'wherehows-web/utils/api/datasets/dataset';
import { readDatasetOwners, updateDatasetOwners } from 'wherehows-web/utils/api/datasets/owners';
import { Tabs } from 'wherehows-web/constants/datasets/shared';
import { action } from 'ember-decorators/object';

const { post, getJSON } = $;

// gradual refactor into es class, hence extends EmberObject instance
export default class extends Controller.extend({
  queryParams: ['urn'],
  /**
   * Reference to the application notifications Service
   * @type {Ember.Service}
   */
  notifications: service(),

  isTable: true,
  hasImpacts: false,
  hasSamples: false,
  currentVersion: '0',
  latestVersion: '0',
  init() {
    setProperties(this, {
      ownerTypes: [],
      userTypes: [
        { name: 'Corporate User', value: 'urn:li:corpuser' },
        { name: 'Group User', value: 'urn:li:corpGroup' }
      ]
    });

    this._super(...arguments);
  },
  isPinot: computed('model.source', function() {
    var model = this.get('model');
    if (model) {
      if (model.source) {
        return model.source.toLowerCase() == 'pinot';
      }
    }
    return false;
  }),
  isHDFS: computed('model.urn', function() {
    var model = this.get('model');
    if (model) {
      if (model.urn) {
        return model.urn.substring(0, 7) == 'hdfs://';
      }
    }
    return false;
  }),
  isSFDC: computed('model.source', function() {
    var model = this.get('model');
    if (model) {
      if (model.source) {
        return model.source.toLowerCase() == 'salesforce';
      }
    }
    return false;
  }),
  lineageUrl: computed('model.id', function() {
    var model = this.get('model');
    if (model) {
      if (model.id) {
        return '/lineage/dataset/' + model.id;
      }
    }
    return '';
  }),
  schemaHistoryUrl: computed('model.id', function() {
    var model = this.get('model');
    if (model) {
      if (model.id) {
        return '/schemaHistory#/schemas/' + model.id;
      }
    }
    return '';
  }),

  refreshVersions: function(dbId) {
    var model = this.get('model');
    if (!model || !model.id) {
      return;
    }
    var versionUrl = '/api/v1/datasets/' + model.id + '/versions/db/' + dbId;
    $.get(versionUrl, data => {
      if (data && data.status == 'ok' && data.versions && data.versions.length > 0) {
        this.set('hasversions', true);
        this.set('versions', data.versions);
        this.set('latestVersion', data.versions[0]);
        this.changeVersion(data.versions[0]);
      } else {
        this.set('hasversions', false);
        this.set('currentVersion', '0');
        this.set('latestVersion', '0');
      }
    });
  },
  changeVersion: function(version) {
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
        } else {
          $(objs[i]).addClass('btn-default');
        }
      }
    }
    var model = this.get('model');
    if (version != latestVersion) {
      if (!model || !model.id) {
        return;
      }
      var schemaUrl = '/api/v1/datasets/' + model.id + '/schema/' + version;
      $.get(schemaUrl, function(data) {
        if (data && data.status == 'ok') {
          setTimeout(function() {
            $('#json-viewer').JSONView(JSON.parse(data.schema_text));
          }, 500);
        }
      });
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
    const request = {
      url: this.getUrlFor(urlId),
      data: JSON.stringify(data),
      contentType: 'application/json'
    };

    // If the return_code is not 200 reject the Promise
    return Promise.resolve(post(request)).then(
      ({ status, msg = '' }) => status === 'ok' || Promise.reject(new Error(msg))
    );
  },

  getUrlFor(urlId) {
    return {
      compliance: () => datasetComplianceUrlById(`${get(this, 'datasetId')}`)
    }[urlId]();
  },

  exceptionOnSave({ message = 'An error occurred' }) {
    const [error] = arguments;
    debug(`An error occurred on while updating : ${message}`);

    throw error;
  },

  async handleDatasetComment(strategy, ...args) {
    const { datasetId: id, 'notifications.notify': notify } = getProperties(this, [
      'datasetId',
      'notifications.notify'
    ]);

    const action = {
      create: createDatasetComment.bind(null, id),
      destroy: deleteDatasetComment.bind(null, id),
      modify: updateDatasetComment.bind(null, id)
    }[strategy];

    try {
      await action(...args);
      notify('success', { content: 'Success!' });
      // refresh the list of comments if successful with updated response
      set(this, 'datasetComments', await readDatasetComments(id));

      return true;
    } catch (e) {
      notify('error', { content: e.message });
    }

    return false;
  },

  actions: {
    /**
     * Updates the dataset's deprecation properties
     * @param {boolean} isDeprecated
     * @param {string} deprecationNote
     * @return {IDatasetView}
     */
    async updateDeprecation(isDeprecated, deprecationNote) {
      const datasetId = get(this, 'datasetId');

      await updateDatasetDeprecation(datasetId, isDeprecated, deprecationNote);
      return set(this, 'datasetView', await readDatasetView(datasetId));
    },

    /**
     * Action handler creates a dataset comment with the type and text pas
     * @param {CommentTypeUnion} type the comment type
     * @param {string} text the text of the comment
     * @return {Promise.<boolean>} true if successful in creating the comment, false otherwise
     */
    async createDatasetComment({ type, text }) {
      return this.handleDatasetComment.call(this, 'create', { type, text });
    },

    /**
     * Deletes a comment from the current dataset
     * @param {number} commentId the id for the comment to be deleted
     * @return {Promise.<boolean>}
     */
    async destroyDatasetComment(commentId) {
      return this.handleDatasetComment.call(this, 'destroy', commentId);
    },

    /**
     * Updates a comment on the current dataset
     * @param commentId
     * @param updatedComment
     * @return {Promise.<boolean>}
     */
    async updateDatasetComment(commentId, updatedComment) {
      return this.handleDatasetComment.call(this, 'modify', commentId, updatedComment);
    },

    /**
     * Persists the list of owners, and if successful, updated the owners
     * on the controller
     * @param {Array<IOwner>} updatedOwners the list of owner to send
     * @returns {Promise<Array<IOwner>>}
     */
    async saveOwnerChanges(updatedOwners) {
      const { datasetId: id, 'notifications.notify': notify } = getProperties(this, [
        'datasetId',
        'notifications.notify'
      ]);
      const csrfToken = getWithDefault(this, 'csrfToken', '').replace('/', '');

      try {
        await updateDatasetOwners(id, csrfToken, updatedOwners);
        notify('success', { content: 'Success!' });

        // updates the shared state for list of dataset owners
        return set(this, 'owners', await readDatasetOwners(id));
      } catch (e) {
        notify('error', { content: e.message });
      }
    },

    updateVersion: function(version) {
      this.changeVersion(version);
    },
    updateInstance: function(instance) {
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
          } else {
            $(objs[i]).addClass('btn-default');
          }
        }
      }

      this.set('currentInstance', instance.dbId);
      this.refreshVersions(instance.dbId);
    },

    /**
     * Requests the privacyCompliancePolicy for the current dataset id
     * and sets the result on the controller `privacyCompliancePolicy` property
     * @returns {Promise.<*>}
     */
    resetPrivacyCompliancePolicy() {
      return getJSON(this.getUrlFor('compliance'), ({ status, complianceInfo }) => {
        status === 'ok' &&
          setProperties(this, {
            complianceInfo,
            isNewComplianceInfo: false
          });
      });
    },

    /**
     * Retrieves the current version of the privacyCompliancePolicy document and invokes an api to persist the document
     * then updates controller state if successful
     */
    savePrivacyCompliancePolicy() {
      return this.saveJson('compliance', get(this, 'complianceInfo'))
        .then(this.actions.resetPrivacyCompliancePolicy.bind(this))
        .catch(this.exceptionOnSave);
    }
  }
}) {
  tabIds = Tabs;

  tabSelected;

  constructor() {
    super();
    this.tabSelected || (this.tabSelected = Tabs.Ownership);
  }

  /**
   * Handles user generated tab selection action by transitioning to specified route
   * @param {Tabs} tabSelected the currently selected tab
   */
  @action
  tabSelectionChanged(tabSelected) {
    // if the tab selection is same as current, noop
    return get(this, 'tabSelected') === tabSelected
      ? void 0
      : this.transitionToRoute(`datasets.dataset.${tabSelected}`, get(this, 'datasetId'));
  }
}
