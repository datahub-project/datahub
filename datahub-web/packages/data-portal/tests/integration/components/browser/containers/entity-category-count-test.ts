import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';
import { IMirageTestContext } from '@datahub/utils/types/vendor/ember-cli-mirage-deprecated';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';

module('Integration | Component | browser/containers/entity-category-count', function(hooks) {
  setupRenderingTest(hooks);

  test('component rendering', async function(assert) {
    assert.expect(2);

    this.set('entity', DatasetEntity.displayName);

    await render(hbs`
      {{#browser/containers/entity-category-count entity=entity}}
        nested container content
      {{/browser/containers/entity-category-count}}
    `);

    assert.ok(this.element, 'expect component to be rendered in DOM');
    assert.equal(
      this.element && this.element.textContent && this.element.textContent.trim(),
      'nested container content',
      'expect container to render nested content'
    );
  });

  test('component rendering with a urn', async function(this: IMirageTestContext, assert) {
    assert.expect(2);

    const { server } = this;
    server.create('datasetView', 10);

    this.set('entity', DatasetEntity.displayName);
    this.setProperties({
      entity: DatasetEntity.displayName,
      category: DatasetPlatform.Ambry
    });

    await render(hbs`
      {{#browser/containers/entity-category-count entity=entity category=category as |container|}}
        {{container.count}}
      {{/browser/containers/entity-category-count}}
    `);

    assert.ok(this.element, 'expect component to be rendered in DOM');

    const yieldedCount = parseInt((this.element && this.element.textContent) || '-1');
    assert.ok(yieldedCount >= 0, 'expect yielded value to be a number greater or equal to 0');
  });
});
