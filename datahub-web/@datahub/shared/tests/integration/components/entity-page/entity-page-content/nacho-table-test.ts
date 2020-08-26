import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { INachoTableComponent } from '@datahub/data-models/types/entity/rendering/page-components';

module('Integration | Component | entity-page/entity-page-content/nacho-table', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    const options: INachoTableComponent<{ prop: Array<string> }>['options'] = {
      emptyStateMessage: 'no data',
      propertyName: 'prop',
      tableConfigs: {
        headers: [],
        labels: []
      }
    };
    const entity = {};
    this.setProperties({
      options,
      entity
    });
    await render(hbs`<EntityPage::EntityPageContent::NachoTable @options={{this.options}} @entity={{this.entity}}/>`);
    assert.equal(this.element.textContent && this.element.textContent.trim(), 'no data');
  });
});
