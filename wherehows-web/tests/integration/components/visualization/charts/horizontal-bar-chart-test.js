import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent(
  'visualization/charts/horizontal-bar-chart',
  'Integration | Component | visualization/charts/horizontal-bar-chart',
  { integration: true }
);

/* Selectors */
const chartTitle = '.viz-bar-chart__title';
const chartBar = 'rect';
const chartLabel = '.highcharts-data-label';

test('it renders', async function(assert) {
  this.render(hbs`{{visualization/charts/horizontal-bar-chart}}`);
  assert.ok(this.$(), 'Renders without errors');
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
  this.render(hbs`{{visualization/charts/horizontal-bar-chart
                    series=series
                    title=title}}`);

  assert.ok(this.$(), 'Still renders without errors');
  assert.equal(this.$(chartBar).length, series.length, 'Renders 3 bars');
  assert.equal(this.$(chartLabel).length, series.length, 'Renders 3 labels');

  assert.equal(
    this.$('text:eq(0)')
      .text()
      .trim()
      .replace(/[ \n]/g, ''),
    '150|Mewtwo',
    'Renders the correct first label'
  );
});
