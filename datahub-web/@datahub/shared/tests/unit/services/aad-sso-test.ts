import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';

module('Unit | Service | aad-sso', function(hooks): void {
  setupTest(hooks);

  test('it exists', function(assert): void {
    const service = this.owner.lookup('service:aad-sso');
    assert.ok(service);
  });
});
