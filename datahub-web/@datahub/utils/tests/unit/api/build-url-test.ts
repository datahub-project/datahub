import { module, test } from 'qunit';
import buildUrl from '@datahub/utils/api/build-url';

module('Unit | Utility | build url', function(): void {
  const baseUrl = 'https://www.linkedin.com';
  const unEncodedString = 'string@string';
  const encodedString = encodeURIComponent(unEncodedString);

  test('baseUrl', function(assert): void {
    let result = buildUrl(baseUrl);
    assert.equal(result, baseUrl, 'it returns the base url when no other arguments are passed');

    result = buildUrl(baseUrl, {});
    assert.equal(result, baseUrl, 'it returns the base url when an empty object is supplied as query parameter');

    result = buildUrl(baseUrl, '');
    assert.equal(result, baseUrl, 'it returns the base url when an empty string is supplied as query parameter');

    result = buildUrl(baseUrl, '', true);
    assert.equal(
      result,
      baseUrl,
      'it returns the base url when the queryParam is an empty string and the useEncoding is set'
    );

    result = buildUrl(baseUrl, 'query', true);
    assert.equal(
      result,
      `${baseUrl}?query=true`,
      'it returns the base url with a query key set to true when true is passed as a query value'
    );

    result = buildUrl(baseUrl, 'query', unEncodedString, true);
    assert.equal(
      result,
      `${baseUrl}?query=${encodedString}`,
      'it returns the encoded value when the useEncoding flag is true'
    );

    result = buildUrl(baseUrl, { query: unEncodedString }, true);
    assert.equal(result, `${baseUrl}?query=${encodedString}`, 'it returns the encoded string on a queryParams object');

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
