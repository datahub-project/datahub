import Ember from 'ember';
import {
  baseCommentEditorOptions,
  exemptPolicy,
  isExempt,
  PurgePolicy,
  purgePolicyProps
} from 'wherehows-web/constants';
import noop from 'wherehows-web/utils/noop';

const { Component, get, set } = Ember;

export default Component.extend({
  tagName: 'ul',

  classNames: ['purge-policy-list'],

  exemptPolicy,

  purgePolicyProps,

  /**
   * The dataset's  platform
   */
  platform: null,

  /**
   * The currently save policy for the dataset purge
   */
  purgePolicy: null,

  /**
   * An options hash for the purge exempt reason text editor
   * @type {}
   */
  editorOptions: {
    ...baseCommentEditorOptions,
    placeholder: {
      text: 'Please provide an explanation for why this dataset is marked "Purge Exempt" status'
    }
  },

  /**
   * Action to handle policy change, by default a no-op function
   * @type {Function}
   */
  onPolicyChange: noop,

  didReceiveAttrs() {
    this._super(...arguments);
    this.checkExemption(get(this, 'purgePolicy'));
  },

  /**
   * Checks that the selected purge policy is exempt, if so, set the
   * flag to request the exemption to true
   * @param {PurgePolicy} purgePolicy
   */
  checkExemption(purgePolicy: PurgePolicy) {
    const exemptionReasonRequested = isExempt(purgePolicy);
    set(this, 'requestExemptionReason', exemptionReasonRequested);
  },

  actions: {
    /**
     * Handles the change to the currently selected purge policy
     * @param {string} _name unused name for the radio group
     * @param {PurgePolicy} purgePolicy the selected purge policy
     */
    onChange(_name: string, purgePolicy: PurgePolicy) {
      return get(this, 'onPolicyChange')(purgePolicy);
    }
  }
});
