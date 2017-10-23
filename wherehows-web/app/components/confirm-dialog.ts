import Component from 'ember-modal-dialog/components/modal-dialog';
import { get } from '@ember/object';

/**
 * The default value for content when component is rendered inline
 * @type {string}
 */
const defaultInlineContent = '';

enum Key {
  Escape = 27
}

export default Component.extend({
  containerClassNames: ['notification-confirm-modal'],

  /**
   * Default value for modal content
   * @type {string}
   */
  content: defaultInlineContent,

  init() {
    this._super(...Array.from(arguments));
  },

  actions: {
    /**
     * Handles user dismissal of modal
     */
    onClose() {
      get(this, 'onClose')();
    },

    /**
     * On user confirmation / affirm
     */
    onConfirm() {
      get(this, 'onConfirm')();
    },

    //TODO: META-91
    onKeyUp({ key, which }: { key: any; which: any }) {
      if (which === Key.Escape || key === 'Escape') {
        get(this, 'onClose')();
      }
    }
  }
});
