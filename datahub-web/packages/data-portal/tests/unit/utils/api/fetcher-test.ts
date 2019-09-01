import { getJSON, postJSON, deleteJSON, putJSON, getHeaders } from '@datahub/utils/api/fetcher';
import { module, test } from 'qunit';
import sinon from 'sinon';
import { TestContext } from 'ember-test-helpers';

module('Unit | Utility | api/fetcher', function(hooks) {
  hooks.beforeEach(function(this: TestContext & { xhr: any }) {
    this.xhr = sinon.useFakeXMLHttpRequest();
  });

  hooks.afterEach(function(this: TestContext & { xhr: any }) {
    this.xhr.restore();
  });

  test('each http request function exists', function(assert) {
    [getJSON, postJSON, deleteJSON, putJSON, getHeaders].forEach(httpRequest =>
      assert.ok(typeof httpRequest === 'function', `${httpRequest} is a function`)
    );
  });

  test('each http request function returns a Promise / thennable', function(assert) {
    [getJSON, postJSON, deleteJSON, putJSON, getHeaders].forEach(httpRequest =>
      assert.ok(
        typeof httpRequest({ url: '' }).then === 'function',
        `${httpRequest} returns a Promise object or thennable`
      )
    );
  });
});
