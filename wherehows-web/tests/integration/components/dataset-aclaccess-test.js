import { module, skip } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | dataset aclaccess', function(hooks) {
  setupRenderingTest(hooks);

  skip('it renders', async function(assert) {
    this.setProperties({
      acls: [],
      accessTypeDropDownOptions: []
    });

    await render(hbs`{{dataset-aclaccess acls=acls accessTypeDropDownOptions=accessTypeDropDownOptions}}`);

    assert.ok(document.querySelector('.acl-permission__header'), 'it renders a constituent element in the DOM');
  });
});
