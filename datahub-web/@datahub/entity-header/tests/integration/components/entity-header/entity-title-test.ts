import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | entity-header/entity-title', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    const title = 'title';
    this.set('title', title);

    await render(hbs`{{entity-header/entity-title}}`);

    assert.equal(this.element.textContent!.trim(), 'UNKNOWN');

    await render(hbs`{{entity-header/entity-title title=title}}`);

    assert.equal(this.element.textContent!.trim(), title);

    await render(hbs`
      {{#entity-header/entity-title}}
        title
      {{/entity-header/entity-title}}
    `);

    assert.equal(this.element.textContent!.trim(), 'title');
  });
});
