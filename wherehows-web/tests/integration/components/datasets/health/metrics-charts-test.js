import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, findAll } from '@ember/test-helpers';
import { run } from '@ember/runloop';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | datasets/health/metrics-charts', function(hooks) {
  setupRenderingTest(hooks);

  const chartClass = '.viz-bar-chart';
  const labelClass = '.highcharts-label';
  const labelValueClass = `${labelClass} .highcharts-emphasized`;
  const barClass = `${chartClass}__bar`;

  test('it renders', async function(assert) {
    await render(hbs`{{datasets/health/metrics-charts}}`);
    assert.ok(this.$(), 'Renders without errors');
  });

  test('it provides the correct information', async function(assert) {
    const categoryData = [{ name: 'Compliance', value: 60 }, { name: 'Ownership', value: 40 }];
    const severityData = [
      { name: 'Minor', value: 50, customColorClass: 'severity-chart__bar--minor' },
      { name: 'Warning', value: 30, customColorClass: 'severity-chart__bar--warning' },
      { name: 'Critical', value: 25, customColorClass: 'severity-chart__bar--critical' }
    ];

    this.setProperties({
      categoryData,
      severityData
    });

    await render(hbs`{{datasets/health/metrics-charts
                      isActiveTab=true
                      categoryData=categoryData
                      severityData=severityData}}`);

    assert.ok(this.$(), 'Still renders without errors');
    assert.equal(findAll(chartClass).length, 2, 'Renders 2 charts');
    assert.equal(
      this.$(`${labelValueClass}:eq(0)`)
        .text()
        .trim(),
      '60%',
      'Renders correct value for labels'
    );
  });
});
