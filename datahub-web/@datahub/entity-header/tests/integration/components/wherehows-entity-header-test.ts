import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | wherehows-entity-header', function(hooks) {
  setupRenderingTest(hooks);

  test('entity header rendering', async function(assert) {
    await render(hbs`{{wherehows-entity-header}}`);

    assert.ok(find('.wherehows-entity-header__content'), 'it renders a contained element with the expected class');

    await render(hbs`
      {{#wherehows-entity-header as |eh|}}
        {{eh.row}}
        {{eh.entityProperty}}
      {{/wherehows-entity-header}}
    `);

    assert.ok(find('.wherehows-entity-header-content-row'), 'it yields the expected row component');
    assert.ok(find('.wherehows-entity-header-property'), 'it yields the expected property component');
  });

  test('entity header properties', async function(assert) {
    const entity = {
      urn: 'urn:li:dataset:(urn:li:dataPlatform:ump,api_platform.api_usage,PROD)'
    };

    this.set('entity', entity);

    await render(hbs`
      {{#wherehows-entity-header entity=entity as |eh|}}
        {{eh.entityTitle title=entity.urn}}
      {{/wherehows-entity-header}}
    `);

    assert.equal(this.element.textContent!.trim(), entity.urn);
  });
});
