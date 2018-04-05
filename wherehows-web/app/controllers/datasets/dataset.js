import Controller from '@ember/controller';
import { computed, set, get, setProperties, getProperties, getWithDefault } from '@ember/object';
import { or } from '@ember/object/computed';
import { debug } from '@ember/debug';
import { inject } from '@ember/service';
import { run, scheduleOnce } from '@ember/runloop';
import {
  datasetComplianceUrlById,
  createDatasetComment,
  readDatasetComments,
  deleteDatasetComment,
  updateDatasetComment
} from 'wherehows-web/utils/api';
import { encodeUrn } from 'wherehows-web/utils/validators/urn';
import { updateDatasetDeprecation } from 'wherehows-web/utils/api/datasets/properties';
import { Tabs } from 'wherehows-web/constants/datasets/shared';
import { action } from 'ember-decorators/object';
import Notifications from 'wherehows-web/services/notifications';

// gradual refactor into es class, hence extends EmberObject instance
export default class extends Controller.extend({
  lineageUrl: computed('model.id', function() {
    var model = this.get('model');
    if (model) {
      if (model.id) {
        return '/lineage/dataset/' + model.id;
      }
    }
    return '';
  }),

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
    }
  }
}) {
  queryParams = ['urn'];

  /**
   * Enum of tab properties
   * @type {Tabs}
   */
  tabIds = Tabs;

  /**
   * The currently selected tab in view
   * @type {Tabs}
   */
  tabSelected;

  /**
   * Flag indicating if the compliance info does not have user entered information
   * @type {boolean}
   */
  isNewComplianceInfo;

  /**
   * Flag indicating is there are suggestions that have not been accepted or ignored
   * @type {boolean}
   */
  hasSuggestions;

  /**
   * Flag indicating there are fields in the compliance policy that have not been updated by a user
   * @type {boolean}
   */
  compliancePolicyHasDrift;

  /**
   * Contains a list of whitelisted dataset platforms for JIT ACL access
   * @type {Array<DatasetPlatform>}
   */
  jitAclAccessWhitelist;

  /**
   * Flag indicating the dataset policy is derived from an upstream source
   * @type {boolean}
   */
  isPolicyFromUpstream = false;

  /**
   * Flag indicating that the compliance policy needs user attention
   * @type {ComputedProperty<boolean>}
   */
  requiresUserAction = or('isNewComplianceInfo', 'hasSuggestions', 'compliancePolicyHasDrift');

  /**
   * Reference to the application notifications Service
   * @type {ComputedProperty<Notifications>}
   */
  notifications = inject();

  /**
   * Converts the uri on a model to a usable URN format
   * @type {ComputedProperty<string>}
   */
  encodedUrn = computed('model', function() {
    const { uri = '' } = get(this, 'model');
    return encodeUrn(uri);
  });

  /**
   * Checks if the current platform exists in the supported list of JIT ACL whitelisted platforms
   * @type {ComputedProperty<boolean>}
   */
  isJitAclAccessEnabled = computed('jitAclAccessWhitelist', function() {
    const jitAclAccessWhitelist = getWithDefault(this, 'jitAclAccessWhitelist', []);
    const { platform } = get(this, 'model');

    return jitAclAccessWhitelist.includes(platform);
  });

  constructor() {
    super(...arguments);
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
      : this.transitionToRoute(`datasets.dataset.${tabSelected}`, get(this, 'encodedUrn'));
  }

  /**
   * Updates the hasSuggestions flag if the policy is not from an upstream dataset, otherwise set to false
   * @param {boolean} hasSuggestions
   */
  @action
  setOnChangeSetChange(hasSuggestions) {
    const fromUpstream = get(this, 'isPolicyFromUpstream');
    set(this, 'hasSuggestions', !fromUpstream && hasSuggestions);
  }

  /**
   * Updates the isNewComplianceInfo flag if the policy is not from an upstream dataset, otherwise set to false
   * Also sets the isPolicyFromUpstream attribute
   * @param {boolean} isNewComplianceInfo
   * @param {boolean} fromUpstream
   */
  @action
  setOnComplianceTypeChange({ isNewComplianceInfo, fromUpstream }) {
    setProperties(this, {
      isNewComplianceInfo: !fromUpstream && isNewComplianceInfo,
      isPolicyFromUpstream: fromUpstream
    });

    if (fromUpstream) {
      this.setOnChangeSetChange(false);
      this.setOnChangeSetDrift(false);
    }
  }

  /**
   * Setter to update the drift flag
   * @param {boolean} hasDrift
   */
  @action
  setOnChangeSetDrift(hasDrift) {
    const fromUpstream = get(this, 'isPolicyFromUpstream');
    set(this, 'compliancePolicyHasDrift', !fromUpstream && hasDrift);
  }
}
