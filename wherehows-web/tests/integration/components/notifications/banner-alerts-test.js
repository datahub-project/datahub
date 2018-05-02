import { moduleForComponent, test } from 'ember-qunit';
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

moduleForComponent('notifications/banner-alerts', 'Integration | Component | notifications/banner alerts', {
  integration: true,
  beforeEach() {
    this.register('service:banners', bannersStub);
    this.inject.service('banners', { as: 'banners' });
  }
});

const bannerAlertClass = '.banner-alert';

test('it renders', function(assert) {
  this.render(hbs`{{notifications/banner-alerts}}`);
  assert.ok(this.$(), 'Renders without errors');
});

test('it renders the correct information', function(assert) {
  this.render(hbs`{{notifications/banner-alerts}}`);
  assert.equal(this.$(bannerAlertClass).length, 2, 'Renders the correct amount of banners');
  assert.equal(
    this.$(`${bannerAlertClass}__content:eq(0)`)
      .text()
      .trim(),
    banners[0].content,
    'Renders the correct text'
  );
  assert.equal(this.$(`.fa-${banners[0].iconName}`).length, 1, 'Renders the correct types of banners');
});
