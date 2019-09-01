import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';
import BannerServiceStub from 'wherehows-web/tests/stubs/services/banner';
import { TestContext } from 'ember-test-helpers';
import { set } from '@ember/object';

module('Unit | Controller | datasets/dataset/tab', function(hooks) {
  setupTest(hooks);

  hooks.beforeEach(function(this: TestContext) {
    this.owner.register('service:banners', BannerServiceStub);
  });

  test('it exists', function(assert) {
    assert.ok(this.owner.lookup('controller:datasets/dataset/tab'));
  });

  test('banner service reflects changes', function(assert) {
    const controller = this.owner.lookup('controller:datasets/dataset/tab');
    const bannerService = this.owner.lookup('service:banners');

    assert.equal(
      controller.banners.isShowingBanners,
      false,
      'expected isShowingBanners property to be of type boolean and false'
    );

    set(bannerService, 'isShowingBanners', true);

    assert.equal(
      controller.banners.isShowingBanners,
      true,
      'expected isShowingBanners property to be of type boolean and true'
    );
  });
});
