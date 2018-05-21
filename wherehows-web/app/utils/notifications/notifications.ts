import { IConfirmOptions } from 'wherehows-web/services/notifications';

/**
 * Defines the interface for properties used in proxy-ing or handling dialog's
 * events async
 * @interface INotificationDialogProps
 */
interface INotificationDialogProps {
  dismissedOrConfirmed: Promise<void>;
  dialogActions: IConfirmOptions['dialogActions'];
}

/**
 * Creates an instance of INotificationDialogProps when invoked. Includes a promise for dismissing or
 * confirming the dialog
 * @returns {INotificationDialogProps}
 */
const notificationDialogActionFactory = (): INotificationDialogProps => {
  let dialogActions = <IConfirmOptions['dialogActions']>{};
  /**
   * Inner function to create a dialog object
   * @template T
   * @param {((value?: T | PromiseLike<T>) => void)} resolveFn
   * @param {(reason?: any) => void} rejectFn
   * @returns {IConfirmOptions.dialogActions}
   */
  const createDialogActions = <T>(
    resolveFn: (value?: T | PromiseLike<T>) => void,
    rejectFn: (reason?: any) => void
  ): IConfirmOptions['dialogActions'] => ({
    didConfirm: () => resolveFn(),
    didDismiss: () => rejectFn()
  });
  const dismissedOrConfirmed = new Promise<void>((resolve, reject): void => {
    dialogActions = { ...dialogActions, ...createDialogActions(resolve, reject) };
  });

  return {
    dismissedOrConfirmed,
    dialogActions
  };
};

export { notificationDialogActionFactory };
