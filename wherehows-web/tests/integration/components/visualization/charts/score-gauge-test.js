import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('visualization/charts/score-gauge', 'Integration | Component | visualization/charts/score-gauge', {
  integration: true
});

const chartContainer = '.score-gauge';
const legendTitle = `${chartContainer}__legend-title`;
const legendValue = `${chartContainer}__legend-value`;

test('it renders', async function(assert) {
  this.render(hbs`{{visualization/charts/score-gauge}}`);
  assert.ok(this.$(), 'Renders without errors');
});

test('it renders the correct inforamtion', async function(assert) {
  const score = 85;
  const title = 'Ash Ketchum';
  this.setProperties({ score, title });

  this.render(hbs`{{visualization/charts/score-gauge
                    score=score
                    title=title}}`);

  assert.ok(this.$(), 'Still renders without erorrs');
  assert.equal(this.$(chartContainer).length, 1, 'Renders one correct element');
  assert.equal(
    this.$(legendTitle)
      .text()
      .trim(),
    title,
    'Renders the correct title'
  );
  assert.equal(
    this.$(legendValue)
      .text()
      .trim(),
    `${score}%`,
    'Renders the score in the correct format'
  );
  assert.equal(this.$(`${legendValue}--good`).length, 1, 'Renders the score in the correct styling');
});
