import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, click } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import sinon from 'sinon';

module('Integration | Component | health/health-insight-recalculation-button', function(hooks): void {
  setupRenderingTest(hooks);

  test('rendering and interactivity', async function(assert): Promise<void> {
    const action = sinon.fake();
    this.setProperties({
      action
    });

    await render(hbs`<Health::HealthInsightRecalculationButton @onRecalculateHealthScore={{this.action}} />`);

    assert.dom().hasText('Recalculate Score');

    await click('button');

    assert.ok(action.calledOnce, "Expected action to be called when the component's button is clicked");
  });
});
