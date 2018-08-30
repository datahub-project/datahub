import { module, test, skip } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import notificationsStub from 'wherehows-web/tests/stubs/services/notifications';
import { noop } from 'wherehows-web/utils/helpers/functions';
module('Integration | Component | datasets/compliance/schema-entities', function(hooks) {
  setupRenderingTest(hooks);
  hooks.beforeEach(function() {
    this.owner.register('service:notifications', notificationsStub);
    this.notifications = this.owner.lookup('service:notifications');
    this.setProperties({
      noop: noop
    });
  });

  // Skipping until development on this actually starts
  skip('it renders', async function(assert) {
    await render(hbs`{{datasets/compliance/schema-entities
                       nextStep=noop
                       notifyOnChangeSetSuggestions=noop
                       notifyOnComplianceSuggestionFeedback=noop
                       notifyOnChangeSetRequiresReview=noop}}`);
    assert.ok(this.element, 'Renders without errors');
    assert.ok(document.querySelector('empty-state'));
  });
});
