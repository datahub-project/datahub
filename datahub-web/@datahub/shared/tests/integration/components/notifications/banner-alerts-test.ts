import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import Service from '@ember/service';
import hbs from 'htmlbars-inline-precompile';
import { TestContext } from 'ember-test-helpers';
import BannerService from '@datahub/shared/services/banners';

const banners = [
  {
    content: 'He told ne enough! He told me you killed him!',
    type: 'info',
    isExiting: false,
    isDimissable: true,
    iconName: 'info-circle'
  },
  {
    content: 'No, Luke, I am your father',
    type: 'confirm',
    isExiting: false,
    isDimissable: true,
    iconName: 'exclamation-circle'
  }
];

// Stubbing the banner service to use in the integration test
const bannersStub = Service.extend({
  banners,
  dequeue() {
    return true;
  }
});

interface IMyTestContext extends TestContext {
  banners: BannerService;
}

module('Integration | Component | notifications/banner alerts', function(hooks) {
  setupRenderingTest(hooks);

  hooks.beforeEach(function(this: IMyTestContext) {
    this.owner.register('service:banners', bannersStub);
    this.banners = this.owner.lookup('service:banners');
  });

  const bannerAlertClass = '.banner-alert';

  test('it renders', async function(assert) {
    await render(hbs`{{notifications/banner-alerts}}`);
    assert.ok(this.element, 'Renders without errors');
  });

  test('it renders the correct information', async function(assert) {
    await render(hbs`{{notifications/banner-alerts}}`);
    assert.dom(bannerAlertClass).exists({ count: 2 }, 'Renders the correct amount of banners');
    assert
      .dom(this.element.querySelector(`${bannerAlertClass}__content`))
      .hasText(banners[0].content, 'Renders the correct text');
    assert.equal(
      this.element.querySelectorAll(`.fa-${banners[0].iconName}`).length,
      1,
      'Renders the correct types of banners'
    );
  });
});
