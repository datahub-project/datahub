import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

const componentClassName = '.nacho-breadcrumbs-container';
const breadcrumbsListClassName = '.nacho-breadcrumbs';

module('Integration | Component | browser/entity-breadcrumbs', function(hooks): void {
  setupRenderingTest(hooks);

  test('Breadcrumbs component rendering', async function(assert): Promise<void> {
    const entity = 'Test Entity';
    const segments: Array<string> = [];

    this.setProperties({ segments, entity });

    await render(hbs`
      <Browser::EntityBreadcrumbs @segments={{segments}} @entity={{entity}} />
    `);

    assert.dom(componentClassName).isVisible();
    assert.dom(componentClassName).hasText(entity);
    const breadcrumbsList = document.querySelector(breadcrumbsListClassName);
    assert.ok(breadcrumbsList && breadcrumbsList.tagName.toLowerCase() === 'ul', 'Expected breadcrumbs to have a list');
  });
});
