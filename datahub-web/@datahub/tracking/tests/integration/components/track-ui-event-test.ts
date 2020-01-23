import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, click } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { assertThrownException } from '@datahub/utils/test-helpers/test-exception';
import { TrackingEventCategory } from '@datahub/tracking/constants/event-tracking';
import { getRenderedComponent } from '@datahub/utils/test-helpers/register-component';
import TrackUiEvent from '@datahub/tracking/components/track-ui-event';
import sinonTest from 'ember-sinon-qunit/test-support/test';
import UnifiedTracking from '@datahub/tracking/services/unified-tracking';

const category = TrackingEventCategory.Entity;
const action = 'testAction';

module('Integration | Component | track-ui-event', function(hooks): void {
  setupRenderingTest(hooks);

  test('instantiation errors are raised as expected', async function(assert): Promise<void> {
    const category = TrackingEventCategory.Entity;

    await assertThrownException(
      assert,
      async (): Promise<void> => {
        await render(hbs`
          <TrackUiEvent>
            <p>Nested Template</p>
          </TrackUiEvent>
        `);
      },
      (err: Error) => err.message.includes('Expected a category to be provided on initialization of TrackUiEvent')
    );

    this.set('category', category);

    await assertThrownException(
      assert,
      async (): Promise<void> => {
        await render(hbs`
          <TrackUiEvent @category={{this.category}}>
            <p>Nested Template</p>
          </TrackUiEvent>
      `);
      },
      (err: Error) => err.message.includes('Expected an action to be provided on initialization of TrackUiEvent')
    );
  });

  test('component renders nested template', async function(assert): Promise<void> {
    this.setProperties({
      action,
      category
    });

    await render(hbs`
      <TrackUiEvent @category={{this.category}} @action={{this.action}}>
        <p>Nested Template</p>
      </TrackUiEvent>
    `);

    assert.dom('p').hasText('Nested Template');
  });

  sinonTest('', async function(this: SinonTestContext, assert): Promise<void> {
    const service: UnifiedTracking = this.owner.lookup('service:unified-tracking');
    const stubbedTrackEvent = this.stub(service, 'trackEvent');

    this.setProperties({
      action,
      category
    });

    const component = await getRenderedComponent({
      ComponentToRender: TrackUiEvent,
      testContext: this,
      template: hbs`
        <TrackUiEvent @category={{this.category}} @action={{this.action}} as |track|>
          <button onclick={{action track.trackOnAction}} />
        </TrackUiEvent>
      `
    });

    await click('button');

    assert.ok(stubbedTrackEvent.called, "Expected the UnifiedTracking service's trackEvent to have been called");

    assert.equal(
      component.tracking,
      service,
      "Expected the component's tracking property to be equal to the UnifiedTracking service"
    );
  });
});
