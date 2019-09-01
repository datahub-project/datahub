import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../templates/components/notifications-toast';
import { layout, classNames } from '@ember-decorators/component';

/**
 * Renders a toast component in the DOM for Notifications of type NotificationType.Toast
 * @export
 * @extends {Component}
 */
@layout(template)
@classNames('notifications__toast')
export default class NotificationsToast extends Component {
  /**
   * Maximum character length for text content in a toast. Text that exceeds this value
   * will be split in the middle and concatenated
   */
  maxToastCharLength = 100;
}
