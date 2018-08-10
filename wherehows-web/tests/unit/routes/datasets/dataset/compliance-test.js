import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';

module('Unit | Route | datasets/dataset/compliance', function(hooks) {
  setupTest(hooks);

  test('it exists', function(assert) {
    let route = this.owner.lookup('route:datasets/dataset/compliance');
    assert.ok(route);
  });
});
