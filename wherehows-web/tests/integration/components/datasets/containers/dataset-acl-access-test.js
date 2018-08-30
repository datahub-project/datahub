import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import notificationsStub from 'wherehows-web/tests/stubs/services/notifications';
import userStub from 'wherehows-web/tests/stubs/services/current-user';
import sinon from 'sinon';

module('Integration | Component | datasets/containers/dataset acl access', function(hooks) {
  setupRenderingTest(hooks);

  hooks.beforeEach(function() {
    this.owner.register('service:current-user', userStub);
    this.owner.register('service:notifications', notificationsStub);

    this['current-user'] = this.owner.lookup('service:current-user');
    this.notifications = this.owner.lookup('service:notifications');

    this.sinonServer = sinon.createFakeServer();
    this.sinonServer.respondImmediately = true;
  });

  hooks.afterEach(function() {
    this.sinonServer.restore();
  });

  test('it renders', async function(assert) {
    this.sinonServer.respondWith('GET', /\/api\/v2\/datasets.*/, [
      200,
      { 'Content-Type': 'application/json' },
      JSON.stringify({})
    ]);
    await render(hbs`{{datasets/containers/dataset-acl-access}}`);

    assert.equal(this.element.textContent.trim(), 'JIT ACL is not currently available for this dataset platform');
  });
});
