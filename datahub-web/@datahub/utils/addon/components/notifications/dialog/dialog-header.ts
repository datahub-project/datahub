import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/notifications/dialog/dialog-header';
import { tagName, layout, classNames } from '@ember-decorators/component';

@layout(template)
@tagName('header')
@classNames('notification-confirm-modal__header')
export default class NotificationsDialogDialogHeader extends Component {}
