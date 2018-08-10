import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, findAll } from '@ember/test-helpers';
import Service from '@ember/service';
import hbs from 'htmlbars-inline-precompile';

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

module('Integration | Component | notifications/banner alerts', function(hooks) {
  setupRenderingTest(hooks);

  hooks.beforeEach(function() {
    this.owner.register('service:banners', bannersStub);
    this.banners = this.owner.lookup('service:banners');
  });

  const bannerAlertClass = '.banner-alert';

  test('it renders', async function(assert) {
    await render(hbs`{{notifications/banner-alerts}}`);
    assert.ok(this.$(), 'Renders without errors');
  });

  test('it renders the correct information', async function(assert) {
    await render(hbs`{{notifications/banner-alerts}}`);
    assert.equal(findAll(bannerAlertClass).length, 2, 'Renders the correct amount of banners');
    assert.equal(
      this.$(`${bannerAlertClass}__content:eq(0)`)
        .text()
        .trim(),
      banners[0].content,
      'Renders the correct text'
    );
    assert.equal(this.$(`.fa-${banners[0].iconName}`).length, 1, 'Renders the correct types of banners');
  });
});
