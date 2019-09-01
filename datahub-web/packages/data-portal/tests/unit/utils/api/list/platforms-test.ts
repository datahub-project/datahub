import { module, test } from 'qunit';
import sinon from 'sinon';
import { readDataPlatforms } from '@datahub/data-models/api/dataset/platforms';
import { TestContext } from 'ember-test-helpers';

module('Unit | Utility | api/list/platforms', function(hooks) {
  hooks.beforeEach(function(this: TestContext & { xhr: any }) {
    this.xhr = sinon.useFakeXMLHttpRequest();
  });

  hooks.afterEach(function(this: TestContext & { xhr: any }) {
    this.xhr.restore();
  });

  test('readPlatforms exhibits expected behaviour', function(assert) {
    assert.ok(typeof readDataPlatforms().then === 'function', 'it returns a Promise object or thennable');
  });
});
