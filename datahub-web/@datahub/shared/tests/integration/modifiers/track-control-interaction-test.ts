import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, click, triggerEvent } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import sinonTest from 'ember-sinon-qunit/test-support/test';
import UnifiedTracking from '@datahub/shared/services/unified-tracking';
import { ControlInteractionType } from '@datahub/shared/constants/tracking/event-tracking/control-interaction-events';
import { stubService } from '@datahub/utils/test-helpers/stub-service';
import { TrackingEventCategory } from '@datahub/shared/constants/tracking/event-tracking';

module('Integration | Modifier | track-control-interaction', function(hooks): void {
  setupRenderingTest(hooks);

  sinonTest('It calls the track event action when triggered', async function(
    this: SinonTestContext,
    assert
  ): Promise<void> {
    const service: UnifiedTracking = this.owner.lookup('service:unified-tracking');
    const stubbedTrackEvent = this.stub(service, 'trackEvent');

    this.setProperties({
      type: ControlInteractionType.click,
      name: 'pika-test'
    });

    await render(hbs`<button {{track-control-interaction type=this.type name=this.name}} type="button" />`);
    await click('button');

    assert.ok(stubbedTrackEvent.called, "Expected the UnifiedTracking service's trackEvent to have been called");
  });

  test('it works as expected with various interaction types', async function(assert) {
    assert.expect(6);
    stubService('unified-tracking', {
      trackEvent(params: { category: TrackingEventCategory.ControlInteraction; action: string }): void {
        assert.equal(
          params.category,
          TrackingEventCategory.ControlInteraction,
          'Passes the correct category parameter'
        );
        assert.equal(params.action, 'pika-test', 'Passes the correct action parameter');
      }
    });

    this.setProperties({
      type: ControlInteractionType.click,
      name: 'pika-test'
    });

    await render(hbs`<button {{track-control-interaction type=this.type name=this.name}} type="button" />`);
    await click('button');

    this.set('type', ControlInteractionType.hover);
    await render(hbs`<button {{track-control-interaction type=this.type name=this.name}} type="button" />`);
    await triggerEvent('button', 'mouseenter');

    this.set('type', ControlInteractionType.focus);
    await render(hbs`<input {{track-control-interaction type=this.type name=this.name}} />`);
    await triggerEvent('input', 'focus');
  });
});
