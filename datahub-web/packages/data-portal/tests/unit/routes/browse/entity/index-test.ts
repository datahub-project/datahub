import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';

module('Unit | Route | browse/entity', function(hooks): void {
  setupTest(hooks);

  test('route should transition for ump metric entity', function(assert): void {
    const route = this.owner.lookup('route:browse/entity');
    let transitionCallCount = 0;

    assert.ok(route, 'browse entity index route instance exists');

    route.set('transitionTo', (_route: string): void => {
      transitionCallCount++;
      assert.equal(_route, 'browse.entity', 'expect route name to be passed as first argument');
    });

    assert.equal(transitionCallCount, 0, 'expect that transitionTo is not called');
  });
});
