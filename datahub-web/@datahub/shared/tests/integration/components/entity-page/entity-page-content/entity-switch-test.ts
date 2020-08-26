import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import Component from '@ember/component';
import { ILinearLayoutComponent, IEntitySwitch } from '@datahub/data-models/types/entity/rendering/page-components';
import { IDynamicComponent } from '@datahub/shared/types/dynamic-component';

class ComponentForEntity1 extends Component {
  layout = hbs`entity1: {{entity.entity1Prop}}`;
}
class ComponentForEntity2 extends Component {
  layout = hbs`entity2: {{entity.entity2Prop}}`;
}
class Entity2 {
  entity2Prop = 'entity2Prop';
}

class Entity1 {
  entity1Prop = 'entity1Prop';
  get entity2(): Entity2 {
    return new Entity2();
  }
}

module('Integration | Component | entity-page/entity-page-content/entity-switch', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    this.owner.register('component:component-for-entity1', ComponentForEntity1);
    this.owner.register('component:component-for-entity2', ComponentForEntity2);

    const component1: IDynamicComponent = {
      name: 'component-for-entity1'
    };
    const component2: IEntitySwitch<Entity1> = {
      name: 'entity-page/entity-page-content/entity-switch',
      options: {
        component: {
          name: 'component-for-entity2'
        },
        propertyName: 'entity2'
      }
    };

    const options: ILinearLayoutComponent<Entity1>['options'] = {
      components: [component1, component2]
    };

    this.setProperties({
      entity: new Entity1(),
      options
    });

    await render(hbs`<EntityPage::Layouts::LinearLayout @entity={{this.entity}} @options={{this.options}} />`);

    assert.dom().containsText('entity1: entity1Prop');
    assert.dom().containsText('entity2: entity2Prop');
  });
});
