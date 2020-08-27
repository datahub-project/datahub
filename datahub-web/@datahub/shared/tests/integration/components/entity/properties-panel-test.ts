import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { IPropertiesPanelArgs } from '@datahub/shared/components/entity/properties-panel';
import { MockEntity } from '@datahub/data-models/entity/mock/mock-entity';
import { IStandardDynamicProperty } from '@datahub/data-models/types/entity/rendering/properties-panel';

module('Integration | Component | entity/properties-panel', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    const mockEntity = new MockEntity('someurn');

    const properties: Array<IStandardDynamicProperty<keyof MockEntity | 'entityLink.link'>> = [
      {
        labelComponent: {
          name: 'entity/properties-panel-label',
          options: {
            displayName: 'Display Name',
            tooltipText: 'tooltip'
          }
        },
        name: 'displayName'
      },
      { displayName: 'Name', name: 'name' },
      {
        displayName: 'Link',
        name: 'entityLink.link',
        component: {
          name: 'link/optional-value'
        }
      }
    ];

    const arg: IPropertiesPanelArgs = {
      entity: mockEntity,
      options: {
        columnNumber: 1,
        standalone: true,
        properties
      }
    };
    this.setProperties(arg);
    await render(hbs`<Entity::PropertiesPanel @entity={{entity}} @options={{options}} />`);

    assert.dom().containsText('Display Name mock-entity Name mock entity Link mock entity');
    assert.dom('[data-columns="1"]').exists();
    assert.dom('[data-standalone="true"]').exists();
    assert.equal(document.querySelector('.nacho-tooltip')?.getAttribute('data-title'), 'tooltip');
  });
});
