import {
  getJSON,
  postJSON,
  deleteJSON,
  putJSON,
  getHeaders,
  fetchAndHandleIfApiError
} from 'wherehows-web/utils/api/fetcher';
import { module, test } from 'qunit';
import sinon from 'sinon';

module('Unit | Utility | api/fetcher', {
  beforeEach() {
    this.xhr = sinon.useFakeXMLHttpRequest();
  },

  afterEach() {
    this.xhr.restore();
  }
});

test('each http request function exists', function(assert) {
  [getJSON, postJSON, deleteJSON, putJSON, getHeaders, fetchAndHandleIfApiError].forEach(httpRequest =>
    assert.ok(typeof httpRequest === 'function', `${httpRequest} is a function`)
  );
});

test('each http request function returns a Promise / thennable', function(assert) {
  [getJSON, postJSON, deleteJSON, putJSON, getHeaders].forEach(httpRequest =>
    assert.ok(typeof httpRequest({}).then === 'function', `${httpRequest} returns a Promise object or thennable`)
  );
});
