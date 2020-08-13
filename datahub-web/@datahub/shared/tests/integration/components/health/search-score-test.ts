import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { settled } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { MockEntity } from '@datahub/data-models/entity/mock/mock-entity';
import HealthSearchScore from '@datahub/shared/components/health/search-score';
import { getRenderedComponent } from '@datahub/utils/test-helpers/register-component';

module('Integration | Component | health/search-score', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    assert.expect(3);

    const value = Math.random();
    const entity = new MockEntity('urn');

    const component = await getRenderedComponent({
      ComponentToRender: HealthSearchScore,
      template: hbs`<Health::SearchScore @value={{this.value}} @entity={{this.entity}} />`,
      testContext: this,
      componentName: 'health/search-score'
    });

    assert.dom().hasText('N/A', 'Expected the value to be N/A when no / empty attributes are passed');

    this.set('value', value);

    await settled();

    assert.dom().hasText(/\d{1,3}%/);

    this.set('entity', entity);

    await settled();

    assert.ok(component.linkParamsToEntityHealthTab, 'Expected linkParamsToEntityHealthTab to have a value');
  });
});
