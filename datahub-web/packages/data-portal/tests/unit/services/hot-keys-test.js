import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';
import { Keyboard } from 'wherehows-web/constants/keyboard';

module('Unit | Service | hot-keys', function(hooks) {
  setupTest(hooks);

  test('it exists', function(assert) {
    const service = this.owner.lookup('service:hot-keys');
    assert.ok(service);
  });

  test('it operates as intended', function(assert) {
    const service = this.owner.lookup('service:hot-keys');
    const theFloorIsLava = () => {
      assert.ok(true, 'The registered function was successfully called');
    };
    const theChairsArePeople = () => {
      assert.ok(false, 'This function should not run after being unregistered');
    };

    assert.expect(2);
    assert.ok(service);
    service.registerKeyMapping(Keyboard.ArrowUp, theFloorIsLava);
    service.applyKeyMapping(Keyboard.ArrowUp);

    service.registerKeyMapping(Keyboard.Enter, theChairsArePeople);
    service.unregisterKeyMapping(Keyboard.Enter);
    service.applyKeyMapping(Keyboard.Enter);
  });
});
