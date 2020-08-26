import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, click, settled } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import sinon from 'sinon';

module('Integration | Component | health/insight-card', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(assert): Promise<void> {
    const onViewHealth = sinon.fake();
    const health = {
      score: null,
      validations: []
    };

    this.setProperties({ onViewHealth, health });

    await render(hbs`<Health::InsightCard @onViewHealth={{this.onViewHealth}} @health={{this.health}} />`);

    assert.dom().hasText('Health No health score available Recalculate Score');

    this.set('health', { ...health, score: 0 });
    assert.dom().hasText('Health 0% See Details');

    await click('button');

    assert.ok(onViewHealth.calledOnce, 'Expected action arg to be called on click interaction');

    this.set('health', { ...health, score: 0.5 });

    await settled();

    assert.dom().hasText('Health 50% See Details');
  });
});
