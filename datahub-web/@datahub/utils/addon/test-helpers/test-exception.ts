import Ember from 'ember';

const originalExceptionHandler = Ember.onerror;

/**
 * Helps test that an expected exception is raised within a QUnit test case
 * The error should be expected under certain conditions while the application is running
 * @param {Assert} assert reference to the QUnit Assert object for the currently running test
 * @param {() => void} raiseException the function to invoke when an exception is expected to be raised
 * @param {(error: Error) => boolean} isValidException a guard to check that the exception thrown matches the expected value, e.g. message equality, referential equality
 */
export const assertThrownException = async (
  assert: Assert,
  raiseException: () => void,
  isValidException: (error: Error) => boolean
): Promise<void> => {
  let isExceptionThrown = false;

  Ember.onerror = (error: Error): void => {
    isExceptionThrown = true;
    if (!isValidException(error)) {
      typeof originalExceptionHandler === 'function' && originalExceptionHandler(error);
    }
  };

  await raiseException();

  // Assert that an exception has to be thrown when this function is invoked otherwise, a non throw is an exception
  assert.ok(isExceptionThrown, 'Expected an exception to be thrown');

  // Restore onerror to state before test assertion
  Ember.onerror = originalExceptionHandler;
};
