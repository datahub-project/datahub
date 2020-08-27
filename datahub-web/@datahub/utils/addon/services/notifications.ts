import Service from '@ember/service';
import { INotification, IToast, IConfirmOptions } from '@datahub/utils/types/notifications/service';
import { NotificationEvent, NotificationType } from '@datahub/utils/constants/notifications';
import { setProperties, set } from '@ember/object';
import { timeout, task } from 'ember-concurrency';
import { action, computed } from '@ember/object';
import {
  notificationDialogActionFactory,
  notificationHandlers,
  isAConfirmationModal
} from '@datahub/utils/lib/notifications';
import { noop } from 'lodash';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import { getOwner } from '@ember/application';

/**
 * Defines the Notifications Service which handles the co-ordination and manages rendering of notification components in the
 * host application
 * @export
 */
export default class Notifications extends Service {
  /**
   * Flag indicating the current notification queue is being processed e.g. waiting for user interaction or being shown in ui
   * This can be used to prevent multiple notifications from being consumed at the same time, e.g. confirmation and a modal
   */
  isBuffering = false;

  /**
   * A "queue" of notifications to be shown to the user.
   * Implemented using a native array, rather than a linked list for perf
   * restrict operations to terminal enqueue and dequeue
   * @private
   */
  notificationsQueue: Array<INotification> = [];

  /**
   * References the notification currently being served. Presentational components can use this to render the
   * notification in the ui
   */
  activeNotification?: INotification;

  /**
   * Indicates if a modal is being shown based on the current notification type, toggled true when this is the case
   */
  isShowingNotification = false;

  /**
   * Convenience flag indicating that the currently active notification is a toast and is being shown
   * @readonly
   * @type {boolean}
   * @memberof Notifications
   */
  @computed('activeNotification', 'isShowingNotification')
  get isShowingToast(): boolean {
    const { isShowingNotification, activeNotification } = this;
    return isShowingNotification && !!activeNotification && activeNotification.type === NotificationType.Toast;
  }

  /**
   * Sets up the head notification in the queue for consumption. Based on the notification type, set the appropriate modal or toast
   * attribute with the notification
   */
  @(task(function*(this: Notifications, notification: INotification): IterableIterator<Promise<void>> {
    const { props } = notification;
    const {
      APP: { notificationsTimeout }
    } = getOwner(this).resolveRegistration('config:environment');

    setProperties(this, { activeNotification: notification, isShowingNotification: true });

    if (!isAConfirmationModal(props)) {
      const { duration = 5 } = props;
      // If an alternate delay is available, use this instead of default duration above, this is useful in scenarios where timing is important, such as testing
      // This can also be modified in tests that assert this behaviour
      const delay = notificationsTimeout ? notificationsTimeout : duration * 1000;
      const toastDelay = timeout(delay);

      // Before dismissal, wait for timeout if the toast is not sticky
      if (!props.isSticky) {
        yield toastDelay;
        this.dismissToast();
      }
    }
  }).restartable())
  setCurrentNotificationTask!: ETaskPromise<void, INotification>;

  /**
   * Takes a notification instance and sets a reference to the current notification on the service,
   * returns a reference to the current notification async
   * @param {INotification} notification
   * @return {Promise<INotification>}
   */
  private async consumeNotification(notification: INotification): Promise<INotification> {
    const promiseToHandleNotification = notification.notificationResolution.createPromiseToHandleThisNotification();
    this.setCurrentNotificationTask.perform(notification);
    await promiseToHandleNotification;
    return notification;
  }

  /**
   * Adds a notification to the back of the queue
   * @private
   */
  private enqueue(notification: INotification, notificationsQueue: Array<INotification>): void {
    notificationsQueue.unshiftObject(notification);
    this.asyncDequeue(notificationsQueue);
  }

  /**
   * Asynchronously and recursively removes notifications from the front of the queue. Waits for each notification in queue
   * to be consumed in turn, starting from the front of the queue
   */
  asyncDequeue(notificationsQueue: Array<INotification>): void {
    if (!this.isBuffering) {
      this.flushBuffer.perform(notificationsQueue);
    }
  }

  /**
   * Recursive helper task to dequeue the notifications queue async while there are pending notifications
   * @private
   */
  @(task(function*(
    this: Notifications,
    notificationsQueue: Array<INotification>
  ): IterableIterator<Promise<INotification>> {
    const notification = notificationsQueue.pop();

    // Queue has been emptied or is empty
    if (notification) {
      try {
        set(this, 'isBuffering', true);
        yield this.consumeNotification(notification);
      } finally {
        set(this, 'isBuffering', false);
        this.flushBuffer.perform(notificationsQueue);
      }
    }

    set(this, 'isBuffering', false);
  }).enqueue())
  private flushBuffer!: ETaskPromise<INotification, Array<INotification>>;

  /**
   * Takes a notification and invokes a handler for that NotificationEvent, then enqueues the notification
   * in the notifications queue
   */
  @action
  notify(props: IConfirmOptions | IToast): void {
    this.enqueue(notificationHandlers[props.type](props), this.notificationsQueue);
  }

  /**
   * Removes the current toast from view and invokes the notification resolution resolver
   */
  @action
  dismissToast(): void {
    const { activeNotification } = this;

    if (activeNotification && !isAConfirmationModal(activeNotification.props)) {
      const { onComplete = noop } = activeNotification.notificationResolution;
      set(this, 'isShowingNotification', false);
      try {
        // If there is a pending set task, which will be for this activeNotification, it should be cancelled
        // This means the timeout for delaying a toast will be ignored
        this.setCurrentNotificationTask.cancelAll();
      } catch {}

      onComplete();
    }
  }

  /**
   * Renders a dialog with the full text of the last IToast instance content,
   * with the option to dismiss the modal
   */
  @action
  showContentDetail(): void {
    const { activeNotification } = this;

    if (activeNotification) {
      const { dialogActions } = notificationDialogActionFactory();
      const { content } = activeNotification.props;

      this.dismissToast();

      this.notify({
        content,
        dialogActions,
        header: 'Notification Detail',
        dismissButtonText: false,
        confirmButtonText: 'Dismiss',
        type: NotificationEvent.confirm
      });
    }
  }

  /**
   * Ignores the modal, invokes the user supplied didDismiss callback
   */
  @action
  dismissModal(): void {
    const { activeNotification } = this;
    if (activeNotification) {
      this.modalAction(activeNotification, 'didDismiss');
    }
  }

  /**
   * Confirms the dialog and invokes the user supplied didConfirm callback
   */
  @action
  confirmModal(): void {
    const { activeNotification } = this;
    if (activeNotification) {
      this.modalAction(activeNotification, 'didConfirm');
    }
  }

  /**
   * DRY method to perform a dismissal or confirm a modal interaction
   */
  modalAction(
    { props, notificationResolution: { onComplete = noop } }: INotification,
    action: 'didDismiss' | 'didConfirm'
  ): void {
    if (isAConfirmationModal(props)) {
      const dismissOrConfirm = props.dialogActions[action];
      set(this, 'isShowingNotification', false);
      dismissOrConfirm();
      onComplete();
    }
  }
}

declare module '@ember/service' {
  // eslint-disable-next-line @typescript-eslint/interface-name-prefix
  interface Registry {
    notifications: Notifications;
  }
}
