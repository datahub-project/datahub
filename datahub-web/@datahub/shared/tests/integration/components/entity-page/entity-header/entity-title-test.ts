import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | entity-page/entity-header/entity-title', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    const title = 'title';
    this.set('title', title);

    await render(hbs`{{entity-page/entity-header/entity-title}}`);

    assert.equal(this.element.textContent?.trim(), 'UNKNOWN');

    await render(hbs`{{entity-page/entity-header/entity-title title=title}}`);

    assert.equal(this.element.textContent?.trim(), title);

    await render(hbs`
      {{#entity-page/entity-header/entity-title}}
        title
      {{/entity-page/entity-header/entity-title}}
    `);

    assert.equal(this.element.textContent?.trim(), 'title');
  });
});
