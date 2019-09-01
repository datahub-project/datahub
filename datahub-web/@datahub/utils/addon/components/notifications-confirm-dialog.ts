import Dialog from 'ember-modal-dialog/components/modal-dialog';
// @ts-ignore: Ignore import of compiled template
import template from '../templates/components/notifications-confirm-dialog';
import { action } from '@ember/object';
import { layout } from '@ember-decorators/component';
import { noop } from 'lodash';

/**
 * The default value for content when component is rendered inline
 */
const defaultInlineContent = '';
const containerClassNames = ['notification-confirm-modal'];

@layout(template)
export default class NotificationsConfirmDialog extends Dialog {
  /**
   * Modal Dialog property for the default inlined content
   */
  content = defaultInlineContent;

  /**
   * Overlay for the modal, dims the application in the background and gives visual focus to the modal
   */
  overlayClass = 'notification-overlay';

  /**
   * Dialog overridden class names
   */
  containerClassNames = containerClassNames;

  /**
   * External closure action
   */
  didClose: () => void = noop;

  /**
   * External closure action
   */
  didConfirm: () => void = noop;

  /**
   * External closure action to handle toggling the tertiary toggle action on the dialog if available
   */
  onDialogToggle: () => void = noop;

  /**
   * Handles the onClose external action
   */
  @action
  onClose(): void {
    this.didClose();
  }

  /**
   * Handles the onConfirm external action
   */
  @action
  onConfirm(): void {
    this.didConfirm();
  }
}
