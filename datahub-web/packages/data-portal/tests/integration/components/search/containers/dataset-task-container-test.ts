import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | search/containers/dataset-task-container', function(hooks) {
  setupRenderingTest(hooks);
  // TODO: [META-7611] While the search flow can be tested in our acceptance testing, we should also have some
  // integration tests here that would be appropriate
  test('it renders', async function(assert) {
    await render(hbs`{{search/containers/dataset-task-container}}`);
    assert.ok(true);
  });
});
