/**
 * The expected type of function that the user has performed or created as a consequence of their actions
 * @example - navigation, click, error message, api call made
 */
export enum UserFunctionType {
  // User navigates to a certain page or tab within a page
  Navigation,
  // User interacts with a UI component
  Interaction,
  // User encountered an error
  Error,
  // User receives some API response
  ApiResponse,
  // Generic tracking event
  Tracking
}
