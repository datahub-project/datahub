import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | entity-page/wherehows-entity-header', function(hooks): void {
  setupRenderingTest(hooks);

  test('entity header rendering', async function(assert): Promise<void> {
    await render(hbs`{{entity-page/wherehows-entity-header}}`);

    assert.ok(find('.wherehows-entity-header__content'), 'it renders a contained element with the expected class');

    await render(hbs`
      {{#entity-page/wherehows-entity-header as |eh|}}
        {{eh.row}}
        {{eh.entityProperty}}
      {{/entity-page/wherehows-entity-header}}
    `);

    assert.ok(find('.wherehows-entity-header-content-row'), 'it yields the expected row component');
    assert.ok(find('.wherehows-entity-header-property'), 'it yields the expected property component');
  });

  test('entity header properties', async function(assert): Promise<void> {
    const entity = {
      urn: 'urn:li:dataset:(urn:li:dataPlatform:ump,api_platform.api_usage,PROD)'
    };

    this.set('entity', entity);

    await render(hbs`
      {{#entity-page/wherehows-entity-header entity=entity as |eh|}}
        {{eh.entityTitle title=entity.urn}}
      {{/entity-page/wherehows-entity-header}}
    `);

    assert.equal(this.element.textContent?.trim(), entity.urn);
  });
});
