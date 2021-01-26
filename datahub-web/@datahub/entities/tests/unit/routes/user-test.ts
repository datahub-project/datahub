import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';

module('Unit | Route | user', function(hooks): void {
  setupTest(hooks);

  test('it exists', function(assert): void {
    const route = this.owner.lookup('route:user');
    assert.ok(route);
  });
});
