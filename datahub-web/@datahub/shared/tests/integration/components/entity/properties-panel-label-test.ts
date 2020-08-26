import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { IPropertyPanelLabelComponentOptions } from '@datahub/data-models/types/entity/rendering/properties-panel';

module('Integration | Component | entity/properties-panel-label', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    const labelOptionsWithoutTooltip: IPropertyPanelLabelComponentOptions = {
      displayName: 'test label'
    };

    const labelOptionsWithTooltip: IPropertyPanelLabelComponentOptions = {
      displayName: 'test label',
      tooltipText: 'test tooltip text'
    };

    this.set('options', labelOptionsWithoutTooltip);
    await render(hbs`<Entity::PropertiesPanelLabel @options={{this.options}}/>`);
    assert.dom().hasText('test label');
    assert.dom('.nacho-tooltip').doesNotExist();

    this.set('options', labelOptionsWithTooltip);
    assert.dom('.nacho-tooltip').exists();
    assert.equal(document.querySelector('.nacho-tooltip')?.getAttribute('data-title'), 'test tooltip text');
  });
});
