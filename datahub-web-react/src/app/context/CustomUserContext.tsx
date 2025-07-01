/**
 * Custom User Context State - This is a custom user context state and can be overriden in specific fork of DataHub.
 * The below type can be customized with specific object properties as well if needed.
 */
export type CustomUserContextState = Record<string, any>;

export const DEFAULT_CUSTOM_STATE: CustomUserContextState = {};
