import Ember from 'ember';

const { Service, set, get, setProperties } = Ember;
/**
 * Flag indicating the current notification queue is being processed
 * @type {boolean}
 */
let isBuffering = false;

/**
 * String enum of available notifications
 */
export enum Notifications {
  success = 'success',
  error = 'error',
  info = 'info',
  confirm = 'confirm'
}

type NotificationEvent = keyof Notifications;

type NotificationType = 'modal' | 'toast';

/**
 * Describes the interface for a confirmation modal object
 */
interface IConfirmOptions {
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
  dismissButtonText?: string;
  isDismissible?: boolean;
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

interface INotificationHandler {
  [prop: string]: (...args: Array<any>) => any;
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
const createDialogAwaiter = (resolver: INotificationResolver): Promise<void> =>
  new Promise(resolve => (resolver['onComplete'] = resolve));

const notificationHandlers: INotificationHandler = {
  /**
   * Creates a confirmation dialog notification instance
   * @param {IConfirmOptions} props the properties for the confirmation notification
   * @return {INotification}
   */
  confirm(props: IConfirmOptions): INotification {
    let notificationResolution: INotificationResolver = {
      get queueAwaiter() {
        return createDialogAwaiter(this);
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
  // TODO: META-91 implement toast handlers
  error(/*{ message, sticky = false }: { message: string; sticky: boolean }*/) {},
  success(/*{ message, sticky = false }: { message: string; sticky: boolean }*/) {},
  info(/*{ message, sticky = false }: { message: string; sticky: boolean }*/) {}
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
const NotificationService = Service.extend({
  isShowingModal: false,

  /**
   * Takes a type of notification and invokes a handler for that event, then enqueues the notification
   * to the notifications queue
   * @param {NotificationEvent} type
   * @param params optional list of parameters for the notification handler
   */
  notify(type: NotificationEvent, ...params: any[]): void {
    if (!(type in proxiedNotifications)) {
      return;
    }

    return enqueue(proxiedNotifications[type](...params), notificationsQueue);
  },

  actions: {
    /**
     * Ignores the modal, invokes the user supplied didDismiss callback
     */
    dismissModal() {
      const { props, notificationResolution: { onComplete } }: INotification = get(this, 'modal');

      if ((<IConfirmOptions>props).dialogActions) {
        const { didDismiss } = (<IConfirmOptions>props).dialogActions;
        set(this, 'isShowingModal', false);
        didDismiss();
        typeof onComplete === 'function' && onComplete();
      }
    },

    /**
     * Confirms the dialog and invokes the user supplied didConfirm callback
     */
    confirmModal() {
      const { props, notificationResolution: { onComplete } }: INotification = get(this, 'modal');
      if ((<IConfirmOptions>props).dialogActions) {
        const { didConfirm } = (<IConfirmOptions>props).dialogActions;
        set(this, 'isShowingModal', false);
        didConfirm();
        typeof onComplete === 'function' && onComplete();
      }
    }
  }
});

// Current Ember type definitions are faulty and do not permit defining the init method on classes
// I created an issue to track this here: https://github.com/typed-ember/ember-typings/issues/18
// Using reopen to work around this in the interim
Ember.Object.reopen.call(NotificationService, {
  init() {
    this._super(...Array.from(arguments));

    // Traps for the Proxy handler
    const invokeInService = {
      get: (target: INotificationHandler, handlerName: keyof INotificationHandler) => {
        return handlerName in target ? target[handlerName].bind(this) : void 0;
      }
    };

    proxiedNotifications = new Proxy(notificationHandlers, invokeInService);

    /**
     * Assign the function to setCurrentNotification which will take
     * a Notification and set it as the current notification based on it's type
     * @param {INotification} notification
     */
    setCurrentNotification = (notification: INotification) => {
      if (notification.type === 'modal') {
        setProperties(this, { modal: notification, isShowingModal: true });
      } else {
        setProperties(this, { toast: notification, isShowingToast: true });
      }
    };
  }
});

export default NotificationService;
