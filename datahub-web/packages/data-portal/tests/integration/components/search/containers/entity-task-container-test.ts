import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';

module('Integration | Component | search/containers/entity-task-container', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    this.setProperties({
      entity: DatasetEntity.displayName,
      fields: DatasetEntity.renderProps.search.attributes
    });

    await render(hbs`{{search/containers/entity-task-container entity=entity fields=fields}}`);
    assert.ok(true);
  });
});
