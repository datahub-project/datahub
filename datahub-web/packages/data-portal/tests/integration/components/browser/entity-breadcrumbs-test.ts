import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

const componentClassName = '.nacho-breadcrumbs-container';
const breadcrumbsListClassName = '.nacho-breadcrumbs';

module('Integration | Component | browser/entity-breadcrumbs', function(hooks) {
  setupRenderingTest(hooks);

  test('Breadcrumbs component rendering', async function(assert) {
    const entity = 'Test Entity';
    let segments: Array<string> = [];

    this.setProperties({ segments, entity });

    await render(hbs`
      {{browser/entity-breadcrumbs segments=segments entity=entity}}
    `);

    assert.dom(componentClassName).isVisible();
    assert.dom(componentClassName).hasText(entity);
    const breadcrumbsList = document.querySelector(breadcrumbsListClassName);
    assert.ok(breadcrumbsList && breadcrumbsList.tagName.toLowerCase() === 'ul', 'Expected breadcrumbs to have a list');
  });
});
