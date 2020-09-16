import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | entity-page/entity-header/entity-property', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    await render(hbs`{{entity-page/entity-header/entity-property}}`);

    assert.ok(find('.wherehows-entity-header-property'), 'it renders the element with expected class name');

    await render(hbs`
      {{#entity-page/entity-header/entity-property as |ee|}}
        {{#ee.propertyName}}
          name
        {{/ee.propertyName}}
        {{#ee.propertyValue}}
          value
        {{/ee.propertyValue}}
      {{/entity-page/entity-header/entity-property}}
    `);

    assert.ok(find('.wherehows-entity-header-property__name'), 'it yields a propertyName component');
    assert.ok(find('.wherehows-entity-header-property__value'), 'it yields a propertyValue component');
    assert.ok(this.element.textContent?.trim().includes('name'), 'expected text "name" is rendered');
    assert.ok(this.element.textContent?.trim().includes('value'), 'expected text "value" is rendered');
  });
});
