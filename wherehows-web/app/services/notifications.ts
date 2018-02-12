import Service from '@ember/service';
import { setProperties, get, set } from '@ember/object';
import { delay } from 'wherehows-web/utils/promise-delay';

/**
 * Flag indicating the current notification queue is being processed
 * @type {boolean}
 */
let isBuffering = false;

// type NotificationEvent = 'success' | 'error' | 'info' | 'confirm';

/**
 * String literal of available notifications
 */
export enum NotificationEvent {
  success = 'success',
  error = 'error',
  info = 'info',
  confirm = 'confirm'
}

/**
 * String literal for notification types
 */
type NotificationType = 'modal' | 'toast';

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
  header: string;
  content: string;
  dismissButtonText?: string;
  confirmButtonText?: string;
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
  // The type if the notification
  type: NotificationType;
  // Object holding the queue state for the notification
  notificationResolution: INotificationResolver;
}

/**
 * Describes the notification resolver interface used in handling the
 * dequeueing of the notification buffer
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
    type: 'toast',
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
    let notificationResolution: INotificationResolver = {
      get queueAwaiter() {
        return createNotificationAwaiter(this);
      }
    };

    // Set default values for button text if none are provided by consumer
    props = { dismissButtonText: 'No', confirmButtonText: 'Yes', ...props };

    return {
      props,
      type: 'modal',
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
const enqueue = (notification: INotification, notificationsQueue: Array<INotification>) => {
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
      if (notification.type === 'modal') {
        setProperties<Notifications, 'modal' | 'isShowingModal'>(this, { modal: notification, isShowingModal: true });
      } else {
        const { props } = notification;
        const toastDelay = delay((<IToast>props).duration);
        setProperties<Notifications, 'toast' | 'isShowingToast'>(this, { toast: notification, isShowingToast: true });

        if (!(<IToast>props).isSticky) {
          await toastDelay;
          // Burn toast
          this.actions.dismissToast.call(this);
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

  actions = {
    /**
     * Removes the current toast from view and invokes the notification resolution resolver
     */
    dismissToast(this: Notifications) {
      const { notificationResolution: { onComplete } }: INotification = get(this, 'toast');
      set(this, 'isShowingToast', false);
      onComplete && onComplete();
    },

    /**
     * Ignores the modal, invokes the user supplied didDismiss callback
     */
    dismissModal(this: Notifications) {
      const { props, notificationResolution: { onComplete } }: INotification = get(this, 'modal');

      if ((<IConfirmOptions>props).dialogActions) {
        const { didDismiss } = (<IConfirmOptions>props).dialogActions;
        set(this, 'isShowingModal', false);
        didDismiss();
        onComplete && onComplete();
      }
    },

    /**
     * Confirms the dialog and invokes the user supplied didConfirm callback
     */
    confirmModal(this: Notifications) {
      const { props, notificationResolution: { onComplete } }: INotification = get(this, 'modal');
      if ((<IConfirmOptions>props).dialogActions) {
        const { didConfirm } = (<IConfirmOptions>props).dialogActions;
        set(this, 'isShowingModal', false);
        didConfirm();
        onComplete && onComplete();
      }
    }
  };
}
