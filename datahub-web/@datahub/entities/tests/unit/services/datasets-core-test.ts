import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';

module('Unit | Service | datasets-core', function(hooks): void {
  setupTest(hooks);

  // Replace this with your real tests.
  test('it exists', function(assert): void {
    const service = this.owner.lookup('service:datasets-core');
    assert.ok(service);
  });
});
