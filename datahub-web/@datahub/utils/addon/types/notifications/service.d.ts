import { NotificationEvent, NotificationType } from '@datahub/utils/constants/notifications';

interface INotificationBaseProps {
  // Content to be displayed in the confirmation dialog
  content?: string;
  type: NotificationEvent;
}

/**
 * Describes the interface for a confirmation modal object
 * @export
 * @interface IConfirmOptions
 */
export interface IConfirmOptions extends INotificationBaseProps {
  // Header text for the confirmation dialog
  header: string;
  // Text for button to dismiss dialog action, if false, button will not be rendered
  dismissButtonText?: string | false;
  // Text for button to confirm dialog action, if false, button will not be rendered
  confirmButtonText?: string | false;
  // Text for the dialog toggle switch, only rendered if present
  toggleText?: string;
  // Handler for the toggle switch change
  onDialogToggle?: (checked: boolean) => unknown;
  // Action handlers for dialog button on dismissal or otherwise
  dialogActions: {
    didConfirm: () => unknown;
    didDismiss: () => unknown;
  };
}

/**
 * Describes the interface for a toast object
 * @export
 * @interface IToast
 */
export interface IToast extends INotificationBaseProps {
  duration?: number;
  dismissButtonText?: string;
  isSticky?: boolean;
}

/**
 * Describes the interface for a notification object
 * @export
 * @interface INotification
 */
export interface INotification {
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
 * @export
 * @interface INotificationResolver
 */
export interface INotificationResolver {
  createPromiseToHandleThisNotification: () => Promise<void>;
  onComplete?: () => void;
}

/**
 * Describes the shape for a notification handler function
 * @export
 * @interface INotificationHandler
 */
export interface INotificationHandler {
  [prop: string]: (...args: Array<unknown>) => INotification;
}

/**
 * Defines the interface for properties used in proxy-ing or handling dialog's
 * events async
 * @interface INotificationDialogProps
 */
export interface INotificationDialogProps {
  dismissedOrConfirmed: Promise<void>;
  dialogActions: IConfirmOptions['dialogActions'];
}
