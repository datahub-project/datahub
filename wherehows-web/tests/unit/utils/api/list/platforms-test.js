import * as platformsApi from 'wherehows-web/utils/api/list/platforms';
import { module, test } from 'qunit';
import sinon from 'sinon';

const { readPlatforms } = platformsApi;

module('Unit | Utility | api/list/platforms', {
  beforeEach() {
    this.xhr = sinon.useFakeXMLHttpRequest();
  },

  afterEach() {
    this.xhr.restore();
  }
});

test('readPlatforms exhibits expected behaviour', function(assert) {
  assert.ok(typeof readPlatforms({}).then === 'function', 'it returns a Promise object or thennable');
});
