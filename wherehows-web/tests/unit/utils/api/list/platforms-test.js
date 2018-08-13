import * as platformsApi from 'wherehows-web/utils/api/list/platforms';
import { module, test } from 'qunit';
import sinon from 'sinon';

const { readPlatforms } = platformsApi;

module('Unit | Utility | api/list/platforms', function(hooks) {
  hooks.beforeEach(function() {
    this.xhr = sinon.useFakeXMLHttpRequest();
  });

  hooks.afterEach(function() {
    this.xhr.restore();
  });

  test('readPlatforms exhibits expected behaviour', function(assert) {
    assert.ok(typeof readPlatforms({}).then === 'function', 'it returns a Promise object or thennable');
  });
});
