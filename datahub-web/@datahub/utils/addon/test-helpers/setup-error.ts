import Ember from 'ember';

/**
 * Will save ember onError so we can safely override it in a tests
 * @param hooks
 */
export const setupErrorHandler = (hooks: NestedHooks, options?: { catchMsgs: Array<string> }): void => {
  // Cache built-in value for onerror handler
  let onError: (error: Error) => void;

  hooks.beforeEach(function(): void {
    const { catchMsgs = [] } = options || {};
    onError = Ember.onerror;
    Ember.onerror = function(error): void {
      const catchError = catchMsgs.any(msg => error.message === msg);
      if (!catchError) {
        throw error;
      }
    };
  });

  hooks.afterEach(function(): void {
    if (onError) {
      Ember.onerror = onError;
    }
  });
};
