import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';
import AvatarService, { IAvatarConfigProps } from '@datahub/shared/services/avatar';

module('Unit | Service | avatar', function(hooks) {
  setupTest(hooks);

  test('it behaves as expected', function(assert) {
    const service: AvatarService = this.owner.lookup('service:avatar');
    assert.ok(service, 'Service exists');

    let hasError = false;

    try {
      service.getConfig('aviUrlPrimary');
    } catch {
      hasError = true;
    } finally {
      assert.ok(hasError, 'Calling service before init throws an error');
    }

    const fakeConfigs: IAvatarConfigProps = { aviUrlPrimary: 'pikachu', aviUrlFallback: 'ash' };

    service.initWithConfigs(fakeConfigs);
    assert.equal(service.getConfig('aviUrlPrimary'), 'pikachu');
  });
});
