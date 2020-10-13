import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';

module('Integration | Component | dynamic-components/entity/field', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    const fakeSeedInformation = { description: `Pikachu's special dataset` };
    const testUnderlyingDataset = new DatasetEntity(
      'pikachu',
      (fakeSeedInformation as unknown) as Com.Linkedin.Dataset.Dataset
    );
    this.setProperties({
      entity: testUnderlyingDataset,
      className: 'test-class'
    });

    await render(hbs`
      <DynamicComponents::Entity::Field
        @options={{hash fieldName="description" className=this.className}}
        @entity={{this.entity}}
      />
    `);

    assert.dom('.test-class').hasText(`Pikachu's special dataset`, 'Renders the expected field and class name');
  });
});
