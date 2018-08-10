import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, findAll } from '@ember/test-helpers';
import { run } from '@ember/runloop';
import hbs from 'htmlbars-inline-precompile';
import healthDetail from 'wherehows-web/mirage/fixtures/health-detail';

module('Integration | Component | datasets/health/score-table', function(hooks) {
  setupRenderingTest(hooks);

  const table = '.dataset-health__score-table';
  const row = `${table}__row`;

  test('it renders', async function(assert) {
    await render(hbs`{{datasets/health/score-table}}`);

    assert.ok(this.$(), 'Renders without errors as standalone component');
  });

  test('it renders the correct information', async function(assert) {
    const tableData = healthDetail;

    this.set('tableData', tableData);
    await render(hbs`{{datasets/health/score-table
                      tableData=tableData}}`);

    assert.ok(this.$(), 'Still renders without errors');
    assert.equal(findAll(table).length, 1, 'Renders one table');
    assert.equal(this.$(row).length, tableData.length, 'Renders one row for every detail data');
  });

  test('it filters correctly', async function(assert) {
    const tableData = healthDetail;
    const numComplianceRows = tableData.reduce((sum, curr) => sum + (curr.category === 'Compliance' ? 1 : 0), 0);

    this.setProperties({
      tableData,
      categoryFilter: 'Compliance',
      severityFilter: ''
    });

    await render(hbs`{{datasets/health/score-table
                      tableData=tableData
                      currentCategoryFilter=categoryFilter
                      currentSeverityFilter=severityFilter}}`);

    assert.ok(this.$(), 'Still renders further without errors');
    assert.equal(this.$(row).length, numComplianceRows, 'Show correct rows based on filter');
  });
});
