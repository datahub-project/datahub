import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/notifications/dialog/dialog-content';
import { tagName, layout, classNames } from '@ember-decorators/component';

@layout(template)
@tagName('section')
@classNames('notification-confirm-modal__content')
export default class NotificationsDialogDialogContent extends Component {}
