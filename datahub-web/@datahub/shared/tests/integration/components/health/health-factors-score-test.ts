import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, settled } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import Faker from 'faker';

module('Integration | Component | health/health-factors-score', function(hooks): void {
  setupRenderingTest(hooks);

  test('rendering a score and no score', async function(assert): Promise<void> {
    this.set('score', undefined);
    await render(hbs`<Health::HealthFactorsScore @field={{this.score}} />`);
    assert.dom().hasText('N/A');

    assert.dom('td').exists('Expected a table cell to be rendered for the component');

    this.set('score', Faker.random.number(100));
    await settled();
    assert.dom('').hasText(/\d{1,3}%/, 'Expected the % symbol to be appended to the score');
  });
});
