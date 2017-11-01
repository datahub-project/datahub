import Component from '@ember/component';
import { get, set } from '@ember/object';
import { run, next } from '@ember/runloop';
import {
  baseCommentEditorOptions,
  exemptPolicy,
  isExempt,
  PurgePolicy,
  purgePolicyProps
} from 'wherehows-web/constants';
import noop from 'wherehows-web/utils/noop';

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
  purgePolicy: <PurgePolicy>'',

  requestExemptionReason: false,

  /**
   * An options hash for the purge exempt reason text editor
   * @type {}
   */
  editorOptions: {
    ...baseCommentEditorOptions,
    placeholder: {
      text: 'Please provide an explanation for why this dataset is marked "Purge Exempt" status',
      hideOnClick: false
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

    if (exemptionReasonRequested) {
      // schedule for a future queue, 'likely' post render
      // this allows us to ensure that editor it visible after the set above has been performed
      run(() => next(this, 'focusEditor'));
    }
  },

  /**
   * Applies cursor / document focus to the purge note text editor
   */
  focusEditor() {
    const exemptionReasonElement = <HTMLElement>get(this, 'element').querySelector('.comment-new__content');

    if (exemptionReasonElement) {
      exemptionReasonElement.focus();
    }
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
