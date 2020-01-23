import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

// TODO: [META-8255] This is a copy of the component from data-portal and should be migrated to a centralized addon
// to share between our other addons. Tests will be written at that time
module('Integration | Component | more-info', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    await render(hbs`{{more-info}}`);
    assert.ok(this.element, '');
  });
});
