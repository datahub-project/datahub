import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';

module('Unit | Service | banners', function(hooks) {
  setupTest(hooks);

  test('it exists', function(assert) {
    const service = this.owner.lookup('service:banners');
    assert.ok(service, 'Existence is a good start');
  });

  test('it operates correctly', async function(assert) {
    const service = this.owner.lookup('service:banners');
    const message = 'Ash Ketchum from Pallet Town';

    service.addBanner('Ash Ketchum from Pallet Town', 'info');

    assert.equal(service.banners.length, 1, 'Created a banner');
    assert.equal(service.banners[0].content, message, 'Creates a banner with the right message');
    assert.equal(service.banners[0].isDismissable, true, 'Creates a banner with the right dismiss');

    await service.dequeue().then(() => {
      assert.equal(service.banners.length, 0, 'Removes a banner correctly');
    });
  });
});
