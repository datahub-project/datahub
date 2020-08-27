import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';
import { NotificationEvent, NotificationType } from '@datahub/utils/constants/notifications';
import sinonTest from 'ember-sinon-qunit/test-support/test';
import { INotificationsTestContext } from '@datahub/utils/types/tests/notifications';
import { timeout } from 'ember-concurrency';

module('Unit | Service | notifications', function(hooks): void {
  setupTest(hooks);
  hooks.beforeEach(function(this: INotificationsTestContext) {
    this.service = this.owner.lookup('service:notifications');
  });

  test('it exists', function(this: INotificationsTestContext, assert): void {
    const { service } = this;
    assert.ok(service, 'Expected Notifications to be a service');

    assert.notOk(service.isShowingNotification, 'Expected isShowingNotification to be false');
    assert.notOk(service.activeNotification, 'Expected service to not have an active notification');
    assert.notOk(service.isBuffering, 'Expected service to not be buffering when not active notification exist');
  });

  sinonTest('Service dequeue invocation arguments', async function(
    this: SinonTestContext & INotificationsTestContext,
    assert
  ): Promise<void> {
    assert.expect(2);
    const { service } = this;
    const stubbedDequeue = this.stub(service, 'asyncDequeue');

    service.notify({ type: NotificationEvent.success, duration: 0 });
    assert.ok(
      stubbedDequeue.calledWith(service.notificationsQueue),
      'Expected the dequeue method to be called with the services notifications queue'
    );

    assert.equal(service.notificationsQueue.length, 1, 'Expected a notification to be placed in the queue');

    await service.setCurrentNotificationTask.last;
  });

  sinonTest('Service state flags post notification', function(
    this: SinonTestContext & INotificationsTestContext,
    assert
  ): void {
    const { service } = this;

    service.notify({ type: NotificationEvent.success, duration: 0 });

    assert.ok(service.isBuffering, 'Expected notifications service to be processing the queue');
    assert.ok(
      service.isShowingNotification,
      'Expected the isShowing flag to be true when there is an active notification'
    );
    assert.ok(service.activeNotification, 'Expected the active notification flag to be true');
  });

  test('toast dismissal', function(this: INotificationsTestContext, assert): void {
    const { service } = this;

    service.notify({ type: NotificationEvent.success, duration: 0 });

    service.dismissToast();
    assert.notOk(service.isShowingNotification, 'Expected notification to be dismissed');
    assert.notOk(service.isShowingToast, 'Expected toast notification to be falsy');
  });

  test('showing toast detail', async function(this: INotificationsTestContext, assert): Promise<void> {
    assert.expect(3);
    const { service } = this;

    service.notify({
      duration: 0,
      type: NotificationEvent.success,
      content:
        'A long string of text for user notification that contains more than the required number of characters to be displayed in a toast'
    });

    // Expect the notification to still be rendered after timeout
    // this matches the expected behavior in the UI where the user asynchronously clicks a button to show more detail
    await timeout(0.25 * 1000);

    service.showContentDetail();

    await service.setCurrentNotificationTask.last;

    assert.ok(service.isShowingNotification, 'Expected the notification flag to be truthy');

    const { activeNotification = { type: null } } = service;
    assert.equal(
      activeNotification.type,
      NotificationType.Modal,
      'Expected the active notification to be a modal when showing content detail'
    );

    assert.notOk(service.isShowingToast, 'Expected the active toast notification indicator flag to be falsy');
  });
});
