import { moduleForComponent, test } from 'ember-qunit';
import { run } from '@ember/runloop';
import hbs from 'htmlbars-inline-precompile';
import healthDetail from 'wherehows-web/mirage/fixtures/health-detail';

moduleForComponent('datasets/health/score-table', 'Integration | Component | datasets/health/score-table', {
  integration: true
});

const table = '.dataset-health__score-table';
const row = `${table}__row`;

test('it renders', async function(assert) {
  this.render(hbs`{{datasets/health/score-table}}`);

  assert.ok(this.$(), 'Renders without errors as standalone component');
});

test('it renders the correct information', async function(assert) {
  const tableData = healthDetail;

  this.set('tableData', tableData);
  this.render(hbs`{{datasets/health/score-table
                    tableData=tableData}}`);

  assert.ok(this.$(), 'Still renders without errors');
  assert.equal(this.$(table).length, 1, 'Renders one table');
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

  this.render(hbs`{{datasets/health/score-table
                    tableData=tableData
                    currentCategoryFilter=categoryFilter
                    currentSeverityFilter=severityFilter}}`);

  assert.ok(this.$(), 'Still renders further without errors');
  assert.equal(this.$(row).length, numComplianceRows, 'Show correct rows based on filter');
});
