import {
  INotificationDialogProps,
  IConfirmOptions,
  IToast,
  INotification,
  INotificationResolver,
  INotificationHandler
} from '@datahub/utils/types/notifications/service';
import { NotificationEvent, NotificationType } from '@datahub/utils/constants/notifications';
import { omit, noop } from 'lodash';

/**
 * Creates an instance of INotificationDialogProps when invoked. Includes a promise for dismissing or
 * confirming the dialog
 * @returns {INotificationDialogProps}
 */
export const notificationDialogActionFactory = (): INotificationDialogProps => {
  let dialogActions: IConfirmOptions['dialogActions'] = {
    didConfirm: noop,
    didDismiss: noop
  };
  /**
   * Inner function to create a dialog object
   */
  const createDialogActions = <T>(
    resolveFn: (value?: T | PromiseLike<T>) => void,
    rejectFn: (reason?: unknown) => void
  ): IConfirmOptions['dialogActions'] => ({
    didConfirm: (): void => resolveFn(),
    didDismiss: (): void => rejectFn()
  });

  const dismissedOrConfirmed = new Promise<void>((resolve, reject): void => {
    dialogActions = { ...dialogActions, ...createDialogActions(resolve, reject) };
  });

  return {
    dismissedOrConfirmed,
    dialogActions
  };
};

// TODO: META-91 Perhaps memoize the resolver. Why? successive invocations with the same resolver should maybe return the same Promise
/**
 * Creates a promise that resolve when the current notification has been consumed
 * @param {INotificationResolver} resolver a reference to the Promise resolve executor
 * @return {Promise<void>}
 */
const createNotificationAwaiter = (resolver: INotificationResolver): Promise<void> =>
  new Promise((resolve): ((value?: void | Promise<void>) => void) => (resolver['onComplete'] = resolve));

/**
 * Takes a set of property attributes and constructs an object that matches the INotification shape
 * @param {IToast} props
 * @return {INotification}
 */
const makeToast = (props: IToast): INotification => {
  const notificationResolution: INotificationResolver = {
    /**
     * Builds a promise reference for this INotification instance
     */
    createPromiseToHandleThisNotification(): Promise<void> {
      return createNotificationAwaiter(this);
    }
  };

  return {
    props,
    type: NotificationType.Toast,
    notificationResolution
  };
};

export const isAConfirmationModal = (
  toastOrConfirmation: IConfirmOptions | IToast
): toastOrConfirmation is IConfirmOptions => {
  return toastOrConfirmation.type === NotificationEvent.confirm;
};

/**
 * Handler functions invoked to create an object instance of an INotification.
 */
export const notificationHandlers: INotificationHandler = {
  /**
   * Creates a confirmation dialog notification instance
   * @param {IConfirmOptions} props the properties for the confirmation notification
   * @return {INotification}
   */
  confirm(props: IConfirmOptions): INotification {
    const notificationResolution: INotificationResolver = {
      createPromiseToHandleThisNotification(): Promise<void> {
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
   * Shows a toast indicating that an exception was encountered performing an operation
   * @param {IToast} props
   * @return {INotification}
   */
  error(props: IToast): INotification {
    return makeToast({ content: 'An error occurred!', ...props, type: NotificationEvent.error, isSticky: true });
  },

  /**
   * Shows are toast indicating an action or event was successful
   * @param {IToast} props
   * @return {INotification}
   */
  success(props: IToast): INotification {
    return makeToast({
      content: 'Success!',
      ...props,
      type: NotificationEvent.success
    });
  },

  /**
   * Shows a toast for informational purposes
   * @param {IToast} props
   * @return {INotification}
   */
  info(props: IToast): INotification {
    return makeToast({ content: 'Something noteworthy happened.', ...props, type: NotificationEvent.info });
  }
};
