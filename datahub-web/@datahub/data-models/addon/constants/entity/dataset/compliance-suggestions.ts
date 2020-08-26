/**
 * String indicating that the user affirms or ignored a field suggestion
 * @export
 * @namespace Dataset
 * @enum {string}
 */
export enum SuggestionIntent {
  accept = 'accept',
  ignore = 'reject'
}

/**
 * Possible states to record the user interaction with a suggestion.
 * @export
 * @namespace Dataset
 * @enum {string}
 */
export enum UserSuggestionInteraction {
  NONE = 'none',
  ACCEPT = 'accepted',
  REJECT = 'rejected'
}
