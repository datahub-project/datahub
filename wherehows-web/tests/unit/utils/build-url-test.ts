import buildUrl from 'wherehows-web/utils/build-url';
import { module, test } from 'qunit';

module('Unit | Utility | build url', function() {
  const baseUrl = 'https://www.linkedin.com';

  test('baseUrl', function(assert) {
    let result = buildUrl();
    assert.equal(result, '', 'returns an empty string when no arguments are passed');

    result = buildUrl(baseUrl, '', '');
    assert.equal(result, baseUrl, 'returns the baseUrl when no query parameter is supplied');

    result = buildUrl(baseUrl, 'search', 'text');
    assert.equal(result, `${baseUrl}?search=text`);

    result = buildUrl(result, 'query', 'text2');
    assert.equal(result, `${baseUrl}?search=text&query=text2`);

    result = buildUrl(baseUrl, {
      keyName1: 'keyValue1',
      keyName2: 2,
      keyName3: true
    });
    assert.equal(result, `${baseUrl}?keyName1=keyValue1&keyName2=2&keyName3=true`);

    result = buildUrl(baseUrl, {
      keyName1: 0,
      keyName3: undefined,
      keyName4: '',
      keyName5: null,
      keyName2: false
    });
    assert.equal(result, `${baseUrl}?keyName1=0&keyName2=false`);
  });
});
