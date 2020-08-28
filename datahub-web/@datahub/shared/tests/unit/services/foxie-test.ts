import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';

module('Unit | Service | foxie', function(hooks) {
  setupTest(hooks);

  // Real tests pending
  test('it exists', function(assert) {
    const service = this.owner.lookup('service:foxie');
    assert.ok(service);
  });
});
