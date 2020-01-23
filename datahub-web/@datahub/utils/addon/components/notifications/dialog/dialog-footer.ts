import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/notifications/dialog/dialog-footer';
import { layout, tagName, classNames } from '@ember-decorators/component';
import { noop } from 'lodash';

@layout(template)
@tagName('footer')
@classNames('notification-confirm-modal__footer')
export default class NotificationsDialogDialogFooter extends Component {
  /**
   * Default handler for dialog confirmation
   */
  onConfirm: () => void = noop;

  /**
   * Default handler for dialog dismissal
   */
  onDismiss: () => void = noop;

  /**
   * Default handler for dialog tertiary action: toggle
   */
  onDialogToggle: () => void = noop;
}
