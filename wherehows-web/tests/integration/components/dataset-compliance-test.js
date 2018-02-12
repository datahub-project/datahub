import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

import notificationsStub from 'wherehows-web/tests/stubs/services/notifications';

moduleForComponent('dataset-compliance', 'Integration | Component | dataset compliance', {
  integration: true,

  beforeEach() {
    this.register('service:notifications', notificationsStub);

    this.inject.service('notifications');
  }
});

test('it renders an empty state component when isCompliancePolicyAvailable is false', function(assert) {
  this.render(hbs`{{dataset-compliance}}`);

  assert.notOk(this.get('isCompliancePolicyAvailable'));

  assert.ok(document.querySelector('empty-state'));
});
