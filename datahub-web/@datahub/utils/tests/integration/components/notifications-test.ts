import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, click, settled } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { INotificationsTestContext } from '@datahub/utils/types/tests/notifications';
import { NotificationEvent } from '@datahub/utils/constants/notifications';
import FakeTimers from '@sinonjs/fake-timers';
import { IToast } from '@datahub/utils/types/notifications/service';
import { renderingDone } from '@datahub/utils/test-helpers/rendering';

const toastBaseClass = '.notifications__toast';
const modalBaseClass = '.notification-confirm-modal';
const longNotificationTextContent =
  'A long string of text for user notification that contains more than the required number of characters to be displayed in a toast';

module('Integration | Component | notifications', function(hooks): void {
  setupRenderingTest(hooks);

  hooks.beforeEach(function(this: INotificationsTestContext) {
    this.service = this.owner.lookup('service:notifications');
  });

  test('Notification Component rendering', async function(this: INotificationsTestContext, assert) {
    const { service } = this;
    this.set('service', service);
    await render(hbs`<Notifications />`);

    assert.dom('[notifications-element]').exists();
  });

  test('Notifications: Toast component rendering & behavior', async function(this: INotificationsTestContext, assert) {
    const { service } = this;
    const msgElement = `${toastBaseClass}__content__msg`;
    const clock = FakeTimers.install();
    const baseNotificationDelay = this.owner.resolveRegistration('config:environment').APP.notificationsTimeout;
    this.set('service', service);

    // Installing the fake timers means the runloop timer will not proceed until the clock ticks
    // therefore, we do not `await render`, but use the renderingDone helper to await settlement of all pending except timers
    render(hbs`<Notifications />`);

    service.notify({ type: NotificationEvent.success });
    await renderingDone();

    assert.dom(msgElement).hasText('Success!');

    assert.dom(`${toastBaseClass}--visible`).exists();
    assert.dom(`${toastBaseClass}--hidden`).doesNotExist();
    await click(`${toastBaseClass}__dismiss`);

    assert.notOk(service.isShowingToast, 'Expected service to not be showing toast');
    assert.dom(`${toastBaseClass}--visible`).doesNotExist();
    assert.dom(`${toastBaseClass}--hidden`).exists();

    service.notify({ type: NotificationEvent.error, duration: 0 });
    await renderingDone();

    assert.dom(msgElement).hasText('An error occurred!');

    // Expect the notification to still be rendered after notification delay
    clock.tick(baseNotificationDelay + 1);
    const { activeNotification = { props: { isSticky: false } } } = service;
    const toast: IToast = activeNotification.props as IToast;
    assert.ok(toast.isSticky, 'Expected an error toast to have the flag isSticky set to true');
    assert.dom(msgElement).hasText('An error occurred!', 'Expected  error toast to be sticky');

    await click(`${toastBaseClass}__dismiss`);

    service.notify({ type: NotificationEvent.info, duration: 0 });
    await renderingDone();

    assert.dom(msgElement).hasText('Something noteworthy happened.');

    await click(`${toastBaseClass}__dismiss`);

    service.notify({ type: NotificationEvent.success, duration: 0 });
    await renderingDone();

    assert.dom(msgElement).hasText('Success!');
    await click(`${toastBaseClass}__dismiss`);
    clock.uninstall();
  });

  test('Notifications: Detail Modal rendering and behavior', async function(this: INotificationsTestContext, assert) {
    await render(hbs`<Notifications />`);

    this.service.notify({
      type: NotificationEvent.success,
      content: longNotificationTextContent
    });
    await settled();

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
