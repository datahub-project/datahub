import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, findAll, find } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | visualization/charts/horizontal-bar-chart', function(hooks) {
  setupRenderingTest(hooks);

  /* Selectors */
  const chartBar = 'rect';
  const chartLabel = '.highcharts-data-label';

  test('it renders', async function(assert) {
    await render(hbs`{{visualization/charts/horizontal-bar-chart}}`);
    assert.ok(this.element, 'Renders without errors');
  });

  test('it displays the correct graph information', async function(assert) {
    const title = 'Pokemon Values';
    const series = [
      { name: 'Mewtwo', value: 150 },
      { name: 'Alakazam', value: 65 },
      { name: 'Pikachu', value: 25 },
      { name: 'Charmander', value: 4 }
    ];

    this.setProperties({ title, series });
    await render(hbs`{{visualization/charts/horizontal-bar-chart
                      series=series
                      title=title}}`);

    assert.ok(this.element, 'Still renders without errors');
    assert.equal(findAll(chartBar).length, series.length, 'Renders 3 bars');
    assert.equal(findAll(chartLabel).length, series.length, 'Renders 3 labels');

    assert.equal(
      find('text')
        .textContent.trim()
        .replace(/[ \n]/g, ''),
      '150|Mewtwo',
      'Renders the correct first label'
    );
  });
});
