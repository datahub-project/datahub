import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { ILinearLayoutComponent } from '@datahub/data-models/types/entity/rendering/page-components';

module('Integration | Component | entity-page/layouts/linear-layout', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    const options: ILinearLayoutComponent<unknown>['options'] = {
      components: [
        {
          name: 'empty-state',
          options: {
            heading: 'empty 1'
          }
        },
        {
          name: 'empty-state',
          options: {
            heading: 'empty 2'
          }
        }
      ]
    };
    const entity = {};
    this.setProperties({
      options,
      entity
    });
    await render(hbs`<EntityPage::Layouts::LinearLayout @options={{this.options}} @entity={{this.entity}} />`);

    assert.dom('.empty-state').isVisible({ count: 2 });
    assert.dom('.empty-state').hasText('empty 1');
    assert.dom('.empty-state:last-child').hasText('empty 2');
  });
});
