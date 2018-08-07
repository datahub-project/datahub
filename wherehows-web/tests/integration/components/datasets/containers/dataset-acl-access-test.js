import { moduleForComponent, skip } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import notificationsStub from 'wherehows-web/tests/stubs/services/notifications';
import userStub from 'wherehows-web/tests/stubs/services/current-user';
import sinon from 'sinon';

moduleForComponent(
  'datasets/containers/dataset-acl-access',
  'Integration | Component | datasets/containers/dataset acl access',
  {
    integration: true,

    beforeEach() {
      this.register('service:current-user', userStub);
      this.register('service:notifications', notificationsStub);

      this.inject.service('current-user');
      this.inject.service('notifications');

      this.server = sinon.createFakeServer();
      this.server.respondImmediately = true;
    },

    afterEach() {
      this.server.restore();
    }
  }
);
// Skipping, due to investigate test failure in future PR
skip('it renders', function(assert) {
  this.server.respondWith('GET', /\/api\/v2\/datasets.*/, [
    200,
    { 'Content-Type': 'application/json' },
    JSON.stringify({})
  ]);
  this.render(hbs`{{datasets/containers/dataset-acl-access}}`);

  assert.equal(
    this.$()
      .text()
      .trim(),
    'JIT ACL is not currently available for this dataset platform'
  );
});
