import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | entity-header/naked-div', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    await render(hbs`{{entity-header/naked-div}}`);

    assert.equal(this.element.textContent!.trim(), '');

    await render(hbs`
      {{#entity-header/naked-div}}
        div content
      {{/entity-header/naked-div}}
    `);

    assert.equal(this.element.textContent!.trim(), 'div content');
  });
});
