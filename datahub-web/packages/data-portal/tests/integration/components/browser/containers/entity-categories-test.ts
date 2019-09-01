import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, find } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { UnWrapPromise } from 'wherehows-web/typings/generic';
import BrowseEntity from 'wherehows-web/routes/browse/entity';
import { setMockConfig, resetConfig } from 'wherehows-web/services/configurator';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';

module('Integration | Component | browser/containers/entity-categories', function(hooks) {
  setupRenderingTest(hooks);

  hooks.beforeEach(function() {
    setMockConfig({
      useNewBrowseDataset: true
    });
  });

  hooks.afterEach(function() {
    resetConfig();
  });

  test('container component render & yielding', async function(assert) {
    assert.expect(2);

    const params: UnWrapPromise<ReturnType<BrowseEntity['model']>> = {
      entity: DatasetEntity.displayName,
      page: 1,
      size: 1
    };

    this.set('params', params);

    await render(hbs`
      {{#browser/containers/entity-categories params=params as |entityContainer|}}
        <div id="entity-cat-nodes">
          {{entityContainer.browsePath.entities.length}}
        </div>
        <div id="entity-cat-definition">
          {{entityContainer.entityType.displayName}}
        </div>
      {{/browser/containers/entity-categories}}
    `);

    assert.equal(
      find('#entity-cat-nodes')!.textContent!.trim(),
      '0',
      'it should yield a nodes attribute with a length of 0'
    );
    assert.equal(
      find('#entity-cat-definition')!.textContent!.trim(),
      DatasetEntity.displayName,
      'it should yield an e with a DatamodelEntity property equal to the application defined constant'
    );
  });
});
