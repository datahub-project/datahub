import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';
import MetricsServiceStub from 'wherehows-web/tests/stubs/services/metrics';
import { TestContext } from 'ember-test-helpers';
import { IBaseTrackingEvent } from 'wherehows-web/typings/app/analytics/event-tracking';
import { TrackingEventCategory, TrackingGoal } from 'wherehows-web/constants/analytics/event-tracking';
import { getPiwikActivityQueue } from 'wherehows-web/utils/analytics/piwik';

module('Unit | Service | tracking', function(hooks) {
  setupTest(hooks);

  hooks.beforeEach(function(this: TestContext) {
    this.owner.register('service:metrics', MetricsServiceStub);
  });

  test('service exists and is registered', function(assert) {
    assert.ok(this.owner.lookup('service:tracking'));
  });

  test('service methods proxy to metrics service and display expected behavior', function(assert) {
    assert.expect(2);
    const trackingService = this.owner.lookup('service:tracking');
    const metricsService = this.owner.lookup('service:metrics');
    const event: IBaseTrackingEvent = {
      category: TrackingEventCategory.Entity,
      action: ''
    };

    metricsService.trackEvent = (trackedEvent: IBaseTrackingEvent) =>
      assert.deepEqual(
        event,
        trackedEvent,
        'expected metrics service trackEvent method to be called by Tracking Service method'
      );

    trackingService.trackEvent(event);

    const activityQueue = getPiwikActivityQueue(true);
    trackingService.trackGoal({ name: TrackingGoal.SatClick });

    assert.deepEqual(
      activityQueue,
      [['trackGoal', TrackingGoal.SatClick]],
      'expected TrackingService#trackGoal to add "trackGoal" to activity queue'
    );
  });
});
