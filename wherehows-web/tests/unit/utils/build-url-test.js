import buildUrl from 'wherehows-web/utils/build-url';
import { module, test } from 'qunit';

module('Unit | Utility | build url');

const baseUrl = 'https://www.linkedin.com';

test('baseUrl', function(assert) {
  let result = buildUrl();
  assert.equal(result, '', 'returns an empty string when no arguments are passed');

  result = buildUrl(baseUrl, '');
  assert.equal(result, baseUrl, 'returns the baseUrl when no query parameter is supplied');

  result = buildUrl(baseUrl, 'search', 'text');
  assert.equal(result, `${baseUrl}?search=text`);

  result = buildUrl(result, 'query', 'text2');
  assert.equal(result, `${baseUrl}?search=text&query=text2`);
});
