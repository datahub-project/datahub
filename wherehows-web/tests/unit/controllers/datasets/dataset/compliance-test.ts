import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';

module('Unit | Controller | datasets/dataset/compliance', function(hooks): void {
  setupTest(hooks);

  test('it exists', function(assert): void {
    let controller = this.owner.lookup('controller:datasets/dataset/compliance');
    assert.ok(controller);
    assert.ok(controller.hasOwnProperty('fieldFilter'), 'fieldFilter queryParam has binding to controller attribute');
  });
});
