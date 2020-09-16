import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { IHydrateEntity, ILinearLayoutComponent } from '@datahub/data-models/types/entity/rendering/page-components';
import Component from '@ember/component';
import { MockEntity } from '@datahub/data-models/entity/mock/mock-entity';
import { stubService } from '@datahub/utils/test-helpers/stub-service';
import sinon from 'sinon';

class Layout1 extends Component {
  layout = hbs`Prop1: {{@entity.entity.prop1}}`;
}

class MyMockEntity extends MockEntity<{ prop1: string }> {}

module('Integration | Component | entity-page/entity-page-content/hidrate-entity', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    const completeEntity = new MyMockEntity('urn');
    completeEntity.entity = { prop1: 'hello' };

    const dataModels = {
      createInstance: sinon.fake.returns(completeEntity)
    };
    this.owner.register('component:layout', Layout1);

    stubService('data-models', dataModels);

    const hydrate: IHydrateEntity = {
      name: 'entity-page/entity-page-content/hydrate-entity',
      options: {
        component: {
          name: 'layout'
        }
      }
    };

    const options: ILinearLayoutComponent<MockEntity>['options'] = {
      components: [hydrate]
    };

    this.setProperties({
      entity: new MockEntity('urn'),
      options
    });

    await render(hbs`<EntityPage::Layouts::LinearLayout @entity={{this.entity}} @options={{this.options}} />`);

    assert.ok(dataModels.createInstance.calledOnce);
    assert.dom().containsText('Prop1: hello');
  });
});
