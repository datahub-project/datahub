/**
 * String enum of notification types
 * @type {string}
 */
export enum NotificationEvent {
  success = 'success',
  error = 'error',
  info = 'info',
  confirm = 'confirm'
}

/**
 * String enum of available notifications
 * @type {string}
 */
export enum NotificationType {
  Modal = 'modal',
  Toast = 'toast'
}

/**
 * Base animation speed for the banner alerts.
 * NOTE: This value corresponds with animation speed value set in styles/abstracts/_variables.scss
 */
export const bannerAnimationSpeed = 0.6;

/**
 * Base message for 2 factor auth banner message for the login screen.
 */
export const twoFABannerMessage =
  'We have enabled 2-factor authentication. Login now requires your password + Symantec VIP Token.';

export const twoFABannerType = NotificationEvent['confirm'];
