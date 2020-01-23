import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { render } from '@ember/test-helpers';

const chartContainer = '.score-gauge';
const legendTitle = `${chartContainer}__legend-title`;
const legendValue = `${chartContainer}__legend-value`;

module('Integration | Component | visualization/charts/score-gauge', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    await render(hbs`{{visualization/charts/score-gauge}}`);
    assert.ok(this.element, 'Renders without errors');
  });

  test('it renders the correct inforamtion', async function(assert) {
    const score = 85;
    const title = 'Ash Ketchum';
    this.setProperties({ score, title });

    await render(hbs`{{visualization/charts/score-gauge
                    score=score
                    title=title}}`);

    assert.ok(this.element, 'Still renders without erorrs');
    assert.equal(this.element.querySelectorAll(chartContainer).length, 1, 'Renders one correct element');
    assert.equal(this.element.querySelector(legendTitle).textContent.trim(), title, 'Renders the correct title');
    assert.equal(
      this.element.querySelector(legendValue).textContent.trim(),
      `${score}%`,
      'Renders the score in the correct format'
    );
    assert.equal(
      this.element.querySelectorAll(`${legendValue}--good`).length,
      1,
      'Renders the score in the correct styling'
    );
  });
});
