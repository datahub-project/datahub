import Service from '@ember/service';
import { setProperties, get, set } from '@ember/object';
import { delay } from 'wherehows-web/utils/promise-delay';
import { action } from '@ember-decorators/object';
import { omit } from 'wherehows-web/utils/object';
import { notificationDialogActionFactory } from 'wherehows-web/utils/notifications/notifications';
import { noop } from 'wherehows-web/utils/helpers/functions';

/**
 * Flag indicating the current notification queue is being processed
 * @type {boolean}
 */
let isBuffering = false;

/**
 * String enum of available notifications
 */
export enum NotificationEvent {
  success = 'success',
  error = 'error',
  info = 'info',
  confirm = 'confirm'
}

/**
 * String enum of notification types
 */
enum NotificationType {
  Modal = 'modal',
  Toast = 'toast'
}

/**
 * Describes the proxy handler for INotifications
 */
interface INotificationHandlerTrap<T, K extends keyof T> {
  get(this: Notifications, target: T, handler: K): T[K];
}

/**
 * Describes the interface for a confirmation modal object
 * @export
 * @interface IConfirmOptions
 */
export interface IConfirmOptions {
  // Header text for the confirmation dialog
  header: string;
  // Content to be displayed in the confirmation dialog
  content: string;
  // Text for button to dismiss dialog action, if false, button will not be rendered
  dismissButtonText?: string | false;
  // Text for button to confirm dialog action, if false, button will not be rendered
  confirmButtonText?: string | false;
  // Text for the dialog toggle switch, only rendered if present
  toggleText?: string;
  // Handler for the toggle switch change
  onDialogToggle?: (checked: boolean) => any;
  // Action handlers for dialog button on dismissal or otherwise
  dialogActions: {
    didConfirm: () => any;
    didDismiss: () => any;
  };
}

/**
 * Describes the interface for a toast object
 */
interface IToast {
  content: string;
  duration?: number;
  dismissButtonText?: string;
  isSticky?: boolean;
  type?: NotificationEvent;
}

/**
 * Describes the interface for a notification object
 */
interface INotification {
  // The properties for the notification
  props: IConfirmOptions | IToast;
  // The type of the notification
  type: NotificationType;
  // Object holding the queue state for the notification
  notificationResolution: INotificationResolver;
}

/**
 * Describes the notification resolver interface used in handling the
 * de-queueing of the notification buffer
 */
interface INotificationResolver {
  queueAwaiter: Promise<void>;
  onComplete?: () => void;
}

/**
 * Describes the shape for a notification handler function
 */
interface INotificationHandler {
  [prop: string]: (...args: Array<any>) => INotification;
}

/**
 * Initializes the notification queue to an empty array
 * @type {Array<INotification>}
 */
const notificationsQueue: Array<INotification> = [];

// TODO: META-91 Perhaps memoize the resolver. Why? successive invocations with the same resolver should maybe return the same Promise
/**
 * Creates a promise that resolve when the current notification has been consumed
 * @param {INotificationResolver} resolver a reference to the Promise resolve executor
 * @return {Promise<void>}
 */
const createNotificationAwaiter = (resolver: INotificationResolver): Promise<void> =>
  new Promise(resolve => (resolver['onComplete'] = resolve));

/**
 * Takes a set of property attributes and constructs an object that matches the INotification shape
 * @param {IToast} props
 * @return {INotification}
 */
const makeToast = (props: IToast): INotification => {
  let notificationResolution: INotificationResolver = {
    /**
     * Builds a promise reference for this INotification instance
     * @return {Promise<void>}
     */
    get queueAwaiter() {
      return createNotificationAwaiter(this);
    }
  };

  return {
    props,
    type: NotificationType.Toast,
    notificationResolution
  };
};

const notificationHandlers: INotificationHandler = {
  /**
   * Creates a confirmation dialog notification instance
   * @param {IConfirmOptions} props the properties for the confirmation notification
   * @return {INotification}
   */
  confirm(props: IConfirmOptions): INotification {
    const notificationResolution: INotificationResolver = {
      get queueAwaiter() {
        return createNotificationAwaiter(this);
      }
    };
    // Set default values for button text if none are provided by consumer
    props = { dismissButtonText: 'No', confirmButtonText: 'Yes', ...props };
    const { dismissButtonText, confirmButtonText, onDialogToggle } = props;
    // Removes dismiss or confirm buttons if set to false
    let resolvedProps: IConfirmOptions = dismissButtonText === false ? omit(props, ['dismissButtonText']) : props;

    resolvedProps = confirmButtonText === false ? omit(props, ['confirmButtonText']) : props;
    resolvedProps = typeof onDialogToggle === 'function' ? props : { ...props, onDialogToggle: noop };

    return {
      props: resolvedProps,
      type: NotificationType.Modal,
      notificationResolution
    };
  },

  /**
   *
   * @param {IToast} props
   * @return {INotification}
   */
  error(props: IToast): INotification {
    return makeToast({ content: 'An error occurred!', ...props, type: NotificationEvent.error, isSticky: true });
  },
  /**
   *
   * @param {IToast} props
   * @return {INotification}
   */
  success(props: IToast): INotification {
    return makeToast({ content: 'Success!', ...props, type: NotificationEvent.success });
  },
  /**
   *
   * @param {IToast} props
   * @return {INotification}
   */
  info(props: IToast): INotification {
    return makeToast({ content: 'Something noteworthy happened.', ...props, type: NotificationEvent.info });
  }
};

/**
 * Takes a notification instance and sets a reference to the current notification on the service,
 * returns a reference to the current notification async
 * @param {INotification} notification
 * @return {Promise<INotification>}
 */
const consumeNotification = function(notification: INotification): Promise<INotification> {
  setCurrentNotification(notification);
  return notification.notificationResolution.queueAwaiter.then(() => notification);
};

/**
 * Performs an async recursive iteration of the notification queue. Waits for each notification in queue
 * to be consumed in turn, starting from the front of the queue. FIFO
 * @param {Array<INotification>} notificationsQueue the queue of notifications
 */
const asyncDequeue = function(notificationsQueue: Array<INotification>) {
  if (isBuffering) {
    return;
  }

  (async function flushBuffer(notification: INotification | void): Promise<void> {
    let flushed;

    // Queue is emptied or empty
    if (!notification) {
      isBuffering = false;
      return;
    }

    try {
      isBuffering = true;
      await consumeNotification(notification);
      isBuffering = false;
    } catch (e) {
      isBuffering = false;
    } finally {
      // Recursively iterate through notifications in the queue
      flushed = flushBuffer(notificationsQueue.pop());
    }

    return void flushed;
  })(notificationsQueue.pop());
};

/**
 * Adds a notification to the end of the queue
 * @param {INotification} notification
 * @param {Array<INotification>} notificationsQueue
 */
const enqueue = (notification: INotification, notificationsQueue: Array<INotification>): void => {
  notificationsQueue.unshift(notification);
  asyncDequeue(notificationsQueue);
};

/**
 * Declares a variable for the function which receives a notification and sets a reference on the service
 */
let setCurrentNotification: (notification: INotification) => void;

/**
 * Declares a variable for the notification handlers Proxy
 */
let proxiedNotifications: INotificationHandler;

/**
 * Defines and exports the notification Service implementation and API
 */
export default class Notifications extends Service {
  isShowingModal = false;
  isShowingToast = false;
  toast: INotification;
  modal: INotification;

  constructor() {
    super(...arguments);

    // Traps for the Proxy handler
    const invokeInService: INotificationHandlerTrap<INotificationHandler, keyof INotificationHandler> = {
      get: (target, handlerName) => target[handlerName].bind(this)
    };

    proxiedNotifications = new Proxy(notificationHandlers, invokeInService);

    /**
     * Assign the function to setCurrentNotification which will take
     * a Notification and set it as the current notification based on it's type
     * @param {INotification} notification
     */
    setCurrentNotification = async (notification: INotification) => {
      const { type, props } = notification;

      if (type === NotificationType.Modal) {
        setProperties(this, { modal: notification, isShowingModal: true });
      } else {
        const toastDelay = delay((<IToast>props).duration);

        setProperties(this, { toast: notification, isShowingToast: true });

        if (!(<IToast>props).isSticky) {
          await toastDelay;
          // Burn toast
          if (!(this.isDestroying || this.isDestroying)) {
            this.dismissToast.call(this);
          }
        }
      }
    };
  }

  /**
   * Takes a type of notification and invokes a handler for that event, then enqueues the notification
   * to the notifications queue
   * @param {NotificationEvent} type
   * @param params optional list of parameters for the notification handler
   */
  notify(type: NotificationEvent, ...params: Array<IConfirmOptions | IToast>): void {
    if (!(type in proxiedNotifications)) {
      return;
    }

    return enqueue(proxiedNotifications[type](...params), notificationsQueue);
  }

  /**
   * Removes the current toast from view and invokes the notification resolution resolver
   * @memberof Notifications
   */
  @action
  dismissToast() {
    const {
      notificationResolution: { onComplete }
    }: INotification = get(this, 'toast');

    set(this, 'isShowingToast', false);
    onComplete && onComplete();
  }

  /**
   * Ignores the modal, invokes the user supplied didDismiss callback
   * @memberof Notifications
   */
  @action
  dismissModal() {
    const {
      props,
      notificationResolution: { onComplete }
    }: INotification = get(this, 'modal');

    if ((<IConfirmOptions>props).dialogActions) {
      const { didDismiss } = (<IConfirmOptions>props).dialogActions;
      set(this, 'isShowingModal', false);
      didDismiss();
      onComplete && onComplete();
    }
  }

  /**
   * Confirms the dialog and invokes the user supplied didConfirm callback
   * @memberof Notifications
   */
  @action
  confirmModal() {
    const {
      props,
      notificationResolution: { onComplete }
    }: INotification = get(this, 'modal');

    if ((<IConfirmOptions>props).dialogActions) {
      const { didConfirm } = (<IConfirmOptions>props).dialogActions;
      set(this, 'isShowingModal', false);
      didConfirm();
      onComplete && onComplete();
    }
  }

  /**
   * Renders a dialog with the full text of the last IToast instance content,
   * with the option to dismiss the modal
   * @memberof Notifications
   */
  @action
  async showContentDetail() {
    const { dialogActions } = notificationDialogActionFactory();
    const {
      props: { content }
    } = get(this, 'toast');

    this.notify(NotificationEvent.confirm, {
      header: 'Notification Detail',
      content,
      dialogActions,
      dismissButtonText: false,
      confirmButtonText: 'Dismiss'
    });

    this.dismissToast.call(this);
  }
}
