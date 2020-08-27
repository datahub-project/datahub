import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, waitFor } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { FeatureEntity } from '@datahub/data-models/entity/feature/feature-entity';
import ListCount, { baseComponentClass } from '@datahub/shared/components/lists/list-count';
import { getRenderedComponent } from '@datahub/utils/test-helpers/register-component';
import { stubService } from '@datahub/utils/test-helpers/stub-service';
import { TestContext } from 'ember-test-helpers';

const className = `.${baseComponentClass}`;
const features: Array<string> = [];

module('Integration | Component | list-count', function(hooks): void {
  setupRenderingTest(hooks);

  hooks.beforeEach(function(this: TestContext) {
    this.set('entityType', FeatureEntity);

    stubService('entity-lists-manager', {
      get entities() {
        return {
          'ml-features': features
        };
      }
    });
  });

  test('ListCount rendering', async function(assert): Promise<void> {
    const component = await getRenderedComponent({
      ComponentToRender: ListCount,
      componentName: 'lists/list-count',
      testContext: this,
      template: hbs`<Lists::ListCount @entityType={{this.entityType}} />`
    });

    assert.ok(typeof baseComponentClass === 'string', 'Expected ListCount module to export a baseComponentClass');
    assert.dom(className).exists();
    assert.dom(className).isNotVisible();

    assert.dom(className).hasText('Ml-feature List (0)');

    assert.dom(className).hasClass(`${baseComponentClass}--dismiss`);

    assert.equal(component.entity, FeatureEntity, 'Expected computed property entity to be the instance entityType');
  });

  test('ListCount component behavior', async function(assert): Promise<void> {
    assert.expect(3);

    features.pushObject('One');

    await render(hbs`<Lists::ListCount @entityType={{this.entityType}} />`);

    assert.dom(className).isVisible();

    assert.dom(className).hasText('Ml-feature List (1)');

    features.popObject();

    await waitFor('.entity-list-count--dismiss');
    assert.dom(className).hasText('Ml-feature List (0)');
  });
});
