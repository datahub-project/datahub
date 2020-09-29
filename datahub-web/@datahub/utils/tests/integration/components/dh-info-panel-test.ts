import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | dh-info-panel', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    await render(hbs`
      <DhInfoPanel as |Panel|>
        <Panel.Title>
          Title
        </Panel.Title>
        <Panel.Description>
          Description
        </Panel.Description>
      </DhInfoPanel>
    `);

    assert.dom().containsText('Title');
    assert.dom().containsText('Description');
  });
});
