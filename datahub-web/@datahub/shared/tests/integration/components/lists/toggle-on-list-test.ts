import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, click } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import ToggleOnList, { baseComponentClass } from '@datahub/shared/components/lists/toggle-on-list';
import { FeatureEntity } from '@datahub/data-models/entity/feature/feature-entity';
import { getRenderedComponent } from '@datahub/utils/test-helpers/register-component';
import { stubService } from '@datahub/utils/test-helpers/stub-service';
import { DataModelEntityInstance } from '@datahub/data-models/constants/entity';
import { setupErrorHandler } from '@datahub/utils/test-helpers/setup-error';
import EntityListsManager from '@datahub/shared/services/entity-lists-manager';
import { IStoredEntityAttrs } from '@datahub/shared/types/lists/list';

const className = `.${baseComponentClass}`;
const dataModelInstance = new FeatureEntity('urn');

module('Integration | Component | toggle-on-list', function(hooks): void {
  setupRenderingTest(hooks);
  setupErrorHandler(hooks);

  hooks.beforeEach(function(): void {
    stubService(
      'entity-lists-manager',
      ((): Partial<EntityListsManager> => {
        const features: Array<DataModelEntityInstance> = [];

        return {
          addToList(instance: DataModelEntityInstance): ReturnType<EntityListsManager['addToList']> {
            features.addObject(instance);
            return this as EntityListsManager;
          },
          removeFromList(instance: DataModelEntityInstance): ReturnType<EntityListsManager['removeFromList']> {
            features.removeObjects([instance]);
            return this as EntityListsManager;
          },
          get entities(): Record<string, ReadonlyArray<IStoredEntityAttrs>> {
            return ({
              'ml-features': features
            } as unknown) as Record<string, ReadonlyArray<IStoredEntityAttrs>>;
          }
        };
      })()
    );
  });

  test('ToggleOnList rendering', async function(assert): Promise<void> {
    assert.expect(2);
    this.set('entity', dataModelInstance);

    await render(hbs`<Lists::ToggleOnList @entity={{this.entity}} />`);

    assert.dom(className).exists();
    assert.dom(className).hasText('Add to list');
  });

  test('ToggleOnList toggle behavior', async function(assert): Promise<void> {
    this.setProperties({ entity: dataModelInstance });

    const component = await getRenderedComponent({
      ComponentToRender: ToggleOnList,
      componentName: 'lists/toggle-on-list',
      testContext: this,
      template: hbs`<Lists::ToggleOnList @entity={{this.entity}} />`
    });

    assert.equal(
      component.entity,
      dataModelInstance,
      'Expected ToggleOnList entity property to be the same instance as dataModelInstance'
    );

    assert.dom(className).hasText('Add to list');

    assert.notOk(component.entityExistsInList, 'Expected property entityExistsInList to be falsey');

    await click(className);

    assert.dom(className).hasText('Remove from list');
    assert.ok(component.entityExistsInList, 'Expected property entityExistsInList to be truthy');

    await click(className);

    assert.dom(className).hasText('Add to list');
  });
});
