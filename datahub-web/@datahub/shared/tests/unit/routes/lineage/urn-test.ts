import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';

module('Unit | Route | lineage/urn', function(hooks) {
  setupTest(hooks);

  test('it exists', function(assert) {
    const route = this.owner.lookup('route:lineage/urn');
    assert.ok(route);
  });
});
