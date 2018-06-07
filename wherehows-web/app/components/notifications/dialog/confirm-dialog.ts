import Dialog from 'ember-modal-dialog/components/modal-dialog';
import { get } from '@ember/object';
import { action } from '@ember-decorators/object';

/**
 * The default value for content when component is rendered inline
 * @type {string}
 */
const defaultInlineContent = '';
const containerClassNames = ['notification-confirm-modal'];

export default class NotificationsDialogConfirmDialog extends Dialog.extend({
  content: defaultInlineContent,

  overlayClass: 'notification-overlay',

  containerClassNames
}) {
  /**
   * Handles the onClose external action
   */
  @action
  onClose() {
    get(this, 'onClose')();
  }

  /**
   * Handles the onConfirm external action
   */
  @action
  onConfirm() {
    get(this, 'onConfirm')();
  }
}
