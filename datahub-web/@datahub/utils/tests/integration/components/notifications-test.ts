import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, click } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { INotificationsTestContext } from '@datahub/utils/types/tests/notifications';
import { NotificationEvent } from '@datahub/utils/constants/notifications';
import { timeout } from 'ember-concurrency';
import { IToast } from '@datahub/utils/types/notifications/service';
const toastBaseClass = '.notifications__toast';
const modalBaseClass = '.notification-confirm-modal';
const longNotificationTextContent =
  'A long string of text for user notification that contains more than the required number of characters to be displayed in a toast';

module('Integration | Component | notifications', function(hooks) {
  setupRenderingTest(hooks);

  hooks.beforeEach(function(this: INotificationsTestContext) {
    this.service = this.owner.lookup('service:notifications');
  });

  test('Notification Component rendering', async function(this: INotificationsTestContext, assert) {
    const { service } = this;
    this.set('service', service);
    await render(hbs`<Notifications @service={{this.service}}/>`);

    assert.dom('[notifications-element]').exists();
  });

  test('Notifications: Toast component rendering & behavior', async function(this: INotificationsTestContext, assert) {
    const { service } = this;
    const msgElement = `${toastBaseClass}__content__msg`;
    this.set('service', service);

    await render(hbs`<Notifications @service={{this.service}}/>`);

    service.notify({ type: NotificationEvent.success });

    assert.dom(`${toastBaseClass}--visible`).exists();
    assert.dom(`${toastBaseClass}--hidden`).doesNotExist();
    await click(`${toastBaseClass}__dismiss`);
    assert.dom(`${toastBaseClass}--visible`).doesNotExist();
    assert.dom(`${toastBaseClass}--hidden`).exists();

    service.notify({ type: NotificationEvent.error, duration: 0 });
    assert.dom(msgElement).hasText('An error occurred!');

    // Expect the notification to still be rendered after timeout
    await timeout(0.25 * 1000);
    const { activeNotification = { props: { isSticky: false } } } = service;
    const toast: IToast = activeNotification.props as IToast;
    assert.ok(toast.isSticky, 'Expected an error toast to have the flag isSticky set to true');
    assert.dom(msgElement).hasText('An error occurred!', 'Expected  error toast to be sticky');

    await click(`${toastBaseClass}__dismiss`);

    service.notify({ type: NotificationEvent.info, duration: 0 });
    assert.dom(msgElement).hasText('Something noteworthy happened.');
    await click(`${toastBaseClass}__dismiss`);

    service.notify({ type: NotificationEvent.success, duration: 0 });

    assert.dom(msgElement).hasText('Success!');
    await click(`${toastBaseClass}__dismiss`);
  });

  test('Notifications: Detail Modal rendering and behavior', async function(this: INotificationsTestContext, assert) {
    await render(hbs`<Notifications @service={{this.service}}/>`);

    this.service.notify({
      type: NotificationEvent.success,
      content: longNotificationTextContent
    });

    assert.dom(modalBaseClass).doesNotExist();
    await click(`${toastBaseClass}__content-detail`);
    assert.dom(`${toastBaseClass}--hidden`).exists();

    assert.dom(modalBaseClass).exists();
    assert.dom(`${modalBaseClass}__heading-text`).hasText('Notification Detail');
    assert.dom(`${modalBaseClass}__content`).hasText(longNotificationTextContent);
    assert.dom(`${modalBaseClass}__footer`).hasText('Dismiss');
    await click(`${modalBaseClass}__footer button`);
    assert.dom(modalBaseClass).doesNotExist();
  });
});
