import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';

module('Unit | Service | dataset-meta', function(hooks) {
  setupTest(hooks);

  test('it exists', function(assert) {
    let service = this.owner.lookup('service:dataset-meta');
    assert.ok(service);

    service.set('healthScore', 50);
    assert.equal(service.get('healthScore'), 50);
  });
});
