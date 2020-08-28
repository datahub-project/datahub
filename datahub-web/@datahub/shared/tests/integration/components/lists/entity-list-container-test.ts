import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { stubService } from '@datahub/utils/test-helpers/stub-service';
import { FeatureEntity } from '@datahub/data-models/entity/feature/feature-entity';
import { getRenderedComponent } from '@datahub/utils/test-helpers/register-component';
import EntityListContainer from '@datahub/shared/components/lists/entity-list-container';
import { singularize } from 'ember-inflector';
import { TestContext } from 'ember-test-helpers';
import { startMirage } from 'dummy/initializers/ember-cli-mirage';
import { IStoredEntityAttrs } from '@datahub/shared/types/lists/list';
import { DataModelName } from '@datahub/data-models/constants/entity';

module('Integration | Component | entity-list-container', function(hooks): void {
  setupRenderingTest(hooks);

  const storage: Array<IStoredEntityAttrs> = [];

  hooks.beforeEach(function(this: TestContext): void {
    this.set('server', startMirage());
    this.set('entityType', FeatureEntity.displayName);

    stubService(
      'entity-lists-manager',
      ((): {} => {
        storage.setObjects([]);

        return {
          addToList(instance: IStoredEntityAttrs): void {
            storage.addObject(instance);
          },
          removeFromList(instance: IStoredEntityAttrs): void {
            storage.removeObjects([instance]);
          },
          get entities(): Record<string, {}> {
            return {
              ['ml-features']: storage
            };
          }
        };
      })()
    );
  });

  hooks.afterEach(function(this: TestContext): void {
    this.get('server').shutdown();
  });

  test('container component attributes', async function(assert): Promise<void> {
    const component = await getRenderedComponent({
      ComponentToRender: EntityListContainer,
      componentName: 'lists/entity-list-container',
      testContext: this,
      template: hbs`<Lists::EntityListContainer @entityType={{this.entityType}} />`
    });

    assert.equal(
      component.name,
      singularize(FeatureEntity.displayName),
      'Expected component name property to be singular inflection of test entity'
    );
    assert.equal(component.entity, FeatureEntity, 'Expected entity to refer to a DataModeEntity');
    assert.ok(
      component.listName.includes(' list'),
      'Expected the listName property to exist on the component and have a "list" string'
    );
    assert.notOk(component.hasMultipleSelected, 'Expected selection flag to be false');
    assert.notOk(component.selectedAll, 'Expected selectedAll flag to be false');
    assert.equal(
      component.list,
      this.owner.lookup('service:entity-lists-manager').entities['ml-features'],
      'Expected the list property be the same reference as the entity lists manager service'
    );
    assert.ok(
      Array.isArray(component.entityListWithLinkAttrs) && !component.entityListWithLinkAttrs.length,
      'Expected component property entityListWithLinkAttrs to be an empty array'
    );
    assert.equal(component.tagName, '', 'Expected container component to be a Fragment');
  });

  test('component behavior', async function(assert): Promise<void> {
    // Add two serialized entities to storage list
    storage.addObjects([
      { urn: 'feature-a', type: 'ml-features' as DataModelName },
      { urn: 'feature-b', type: 'ml-features' as DataModelName }
    ]);

    const component = await getRenderedComponent({
      ComponentToRender: EntityListContainer,
      componentName: 'lists/entity-list-container',
      testContext: this,
      template: hbs`<Lists::EntityListContainer @entityType={{this.entityType}} />`
    });

    assert.equal(
      component.instances.length,
      storage.length,
      'Expected component to be instantiated with an equal number of entities from storage'
    );

    // Selected first entity
    component.onSelectEntity(component.instances[0]);

    assert.equal(
      component.listCount,
      storage.length,
      'Expected component property listCount to match length of entity list'
    );
    assert.notOk(
      component.hasMultipleSelected,
      'Expected hasMultipleSelected flag to be false when only one entity is selected'
    );
    assert.equal(
      component.selectedEntities.length,
      1,
      'Expected the component property selectedEntities to have a length equal to the number of selected entities (1)'
    );

    assert.equal(
      component.selectedEntities[0],
      component.instances[0],
      'Expected the entity in the component selectedEntities list to refer to the selected entity instance'
    );

    // Select second entity
    component.onSelectEntity(component.instances[1]);

    assert.ok(
      component.hasMultipleSelected,
      'Expected hasMultipleSelected flag to be true when more than one entity is selected'
    );
    assert.equal(component.selectedEntities.length, 2, 'Expected the component property selectedEntities to be two');

    assert.ok(
      component.selectedAll,
      'Expected selectedAll flag to be true when the selectedEntities length is equal to length of entities'
    );

    // Deselect first entity
    component.onSelectEntity(component.instances[0]);

    assert.notOk(
      component.hasMultipleSelected,
      'Expected hasMultipleSelected flag to be false when only one entity is selected after deselection of entity'
    );
    assert.equal(
      component.selectedEntities.length,
      1,
      'Expected the component property selectedEntities to have a length equal to the number of selected entities (1) after deselection of entity'
    );

    assert.equal(
      component.selectedEntities[0],
      component.instances[1],
      'Expected the entity in the component selectedEntities list to refer to the selected entity instance, after deselection of first entity'
    );

    // Remove all selected entities
    component.removeAllSelected();

    await component.hydrateEntitiesTask.last;

    assert.notOk(component.selectedAll, 'Expected the selectedAll flag to be false when all entities are deselected');
    assert.ok(
      component.selectedEntities.length === 0,
      'Expected the selectedEntities length to be zero when all entities are deselected'
    );

    component.onSelectEntityList();

    assert.ok(component.selectedAll, 'Expected the selectedAll flag to be true when onSelectEntityList is called');
    assert.equal(
      component.selectedEntities.length,
      storage.length,
      'Expected the selectedEntities length to match the storage length when onSelectEntityList is called'
    );
  });

  test('component yield values', async function(assert): Promise<void> {
    const component = await getRenderedComponent({
      ComponentToRender: EntityListContainer,
      componentName: 'lists/entity-list-container',
      testContext: this,
      template: hbs`
      <Lists::EntityListContainer @entityType={{this.entityType}}  as |container|>
        <div class='test-div'>
          {{container.name}}
          {{container.count}}
          {{container.dataModel.displayName}}
          {{container.listName}}
          {{container.selectedAll}}
          {{container.hasMultipleSelected}}
        </div>
      </Lists::EntityListContainer>
      `
    });

    assert
      .dom('div.test-div')
      .hasText(
        `${component.name} ${component.listCount} ${FeatureEntity.displayName} ${component.listName} false false`
      );
  });
});
