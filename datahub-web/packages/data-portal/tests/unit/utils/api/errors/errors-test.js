import { throwIfApiError, ApiError } from 'wherehows-web/utils/api/errors/errors';
import { module, test } from 'qunit';
import { ApiResponseStatus } from 'wherehows-web/utils/api/shared';

module('Unit | Utility | api/errors/errors', function() {
  test('throwIfApiError exists', function(assert) {
    assert.ok(typeof throwIfApiError === 'function', 'throwIfApiError exists as a function');
  });

  test('throwIfApiError returns a Promise / thennable', function(assert) {
    assert.ok(
      typeof throwIfApiError({ status: 200, ok: true, json: () => Promise.resolve() }).then === 'function',
      'invocation returns a Promise object / thennable'
    );
  });

  test('ApiError subclasses built-in Error and has attributes', function(assert) {
    const status = ApiResponseStatus.NotFound;
    const apiError = new ApiError(status);

    assert.ok(apiError instanceof Error, 'is an instanceof Error');
    assert.ok(apiError.timestamp instanceof Date, 'has a valid timestamp');
    assert.ok(apiError.status === status, 'has a status attribute');
  });
});
