import { module, test } from 'qunit';
import { debounceAndMemoizeAsyncQuery } from '@datahub/utils/api/autocomplete';

module('Unit | Utility | debounceAndMemoizeAsyncQuery', function() {
  test('basic usecase', async function(assert): Promise<void> {
    let callTimes = 0;

    const apiFn = (): Promise<boolean> => {
      callTimes += 1;
      return Promise.resolve(true);
    };
    const fn = debounceAndMemoizeAsyncQuery(apiFn);

    // debounce
    fn({ cacheKey: 'testtest', query: 'testtest', requestParams: [] });
    fn({ cacheKey: 'testtes', query: 'testtes', requestParams: [] });
    fn({ cacheKey: 'testt', query: 'testt', requestParams: [] });
    await fn({ cacheKey: 'test', query: 'test', requestParams: [] });
    assert.equal(callTimes, 1);

    // cached
    await fn({ cacheKey: 'test', query: 'test', requestParams: [] });
    assert.equal(callTimes, 1);

    // query threshold
    let response = await fn({ cacheKey: 't', query: 't', requestParams: [] });
    assert.equal(callTimes, 1);
    assert.equal(response, undefined);

    response = await fn({ cacheKey: 't2', query: 't2', requestParams: [], queryLengthThreshold: 0 });
    assert.equal(callTimes, 2);
    assert.equal(response, true);
  });
});
