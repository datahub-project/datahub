import { module, test } from 'qunit';
import { throwIfApiError, ApiError } from '@datahub/utils/api/error';
import { ApiResponseStatus } from '@datahub/utils/api/shared';

module('Unit | Utility | api/errors/errors', function(): void {
  test('throwIfApiError exists', function(assert): void {
    assert.ok(typeof throwIfApiError === 'function', 'throwIfApiError exists as a function');
  });

  test('throwIfApiError returns a Promise / thennable', function(assert): void {
    assert.ok(
      typeof throwIfApiError(
        ({ status: 200, ok: true, json: (): Promise<void> => Promise.resolve() } as unknown) as Response,
        (): Promise<void> => Promise.resolve()
      ).then === 'function',
      'invocation returns a Promise object / thennable'
    );
  });

  test('ApiError subclasses built-in Error and has attributes', function(assert): void {
    const status = ApiResponseStatus.NotFound;
    const apiError = new ApiError(status, '');

    assert.ok(apiError instanceof Error, 'is an instanceof Error');
    assert.ok(apiError.timestamp instanceof Date, 'has a valid timestamp');
    assert.ok(apiError.status === status, 'has a status attribute');
  });
});
