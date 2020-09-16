import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { IEntityDiscriminator } from '@datahub/data-models/types/entity/rendering/page-components';
import Component from '@ember/component';

class Layout1 extends Component {
  layout = hbs`layout1`;
}
class Layout2 extends Component {
  layout = hbs`layout2`;
}

module('Integration | Component | entity-page/entity-page-content/entity-discriminator', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    this.owner.register('component:layout1', Layout1);
    this.owner.register('component:layout2', Layout2);
    const entity1 = {
      type: 'ONE'
    };
    const entity2 = {
      type: 'TWO'
    };
    const options: IEntityDiscriminator<typeof entity1>['options'] = {
      propertyName: 'type',
      default: { name: 'layout1' },
      discriminator: {
        TWO: { name: 'layout2' }
      }
    };
    this.setProperties({ entity: entity1, options });
    await render(
      hbs`<EntityPage::EntityPageContent::EntityDiscriminator @options={{this.options}} @entity={{this.entity}}/>`
    );

    assert.dom().containsText('layout1');
    assert.dom().doesNotContainText('layout2');

    this.set('entity', entity2);

    assert.dom().containsText('layout2');
    assert.dom().doesNotContainText('layout1');
  });
});
