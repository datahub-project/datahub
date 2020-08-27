import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../templates/components/notifications';
import { layout, attribute } from '@ember-decorators/component';
import { NotificationType } from '@datahub/utils/constants/notifications';
import NotificationsService from '@datahub/utils/services/notifications';
import { inject as service } from '@ember/service';
/**
 * Defines the class for the parent notifications component which host the rendering of other notification components within itself e.g. toasts, dialogs
 * @export
 * @extends {Component}
 */
@layout(template)
export default class Notifications extends Component {
  /**
   * Attribute identifier for this component's element
   */
  @attribute('notifications-element')
  readonly notificationsElement = '';

  /**
   * References the NotificationType enum allowing read from the template to determine which sub component is rendered based on the type
   */
  notificationType = NotificationType;

  /**
   * Injected reference to the applications notifications service
   */
  @service('notifications')
  service!: NotificationsService;
}
