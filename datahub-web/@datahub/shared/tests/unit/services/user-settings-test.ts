import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';

module('Unit | Service | user-settings', function(hooks) {
  setupTest(hooks);

  // Real tests pending
  test('it exists', function(assert) {
    const service = this.owner.lookup('service:user-settings');
    assert.ok(service);
  });
});
