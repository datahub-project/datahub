import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';

module('Integration | Helper | get-field-spec', function(hooks) {
  setupRenderingTest(hooks);

  test('it works with our current entity definitions', async function(assert) {
    this.setProperties({ fields: DatasetEntity.renderProps.search.attributes });

    await render(hbs`{{get (get-field-spec fields "name") "displayName"}}`);
    assert.equal(this.element.textContent!.trim(), 'name', 'Gets the field spec as expected');
  });
});
