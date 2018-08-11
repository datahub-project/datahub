import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

import notificationsStub from 'wherehows-web/tests/stubs/services/notifications';

module('Integration | Component | dataset compliance', function(hooks) {
  setupRenderingTest(hooks);

  hooks.beforeEach(function() {
    this.owner.register('service:notifications', notificationsStub);

    this.notifications = this.owner.lookup('service:notifications');
  });

  test('it renders an empty state component when isCompliancePolicyAvailable is false', async function(assert) {
    await render(hbs`{{dataset-compliance}}`);

    assert.notOk(this.get('isCompliancePolicyAvailable'));

    assert.ok(document.querySelector('empty-state'));
  });
});
