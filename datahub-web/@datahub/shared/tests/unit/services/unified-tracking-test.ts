import { module, test } from 'qunit';
import { setupTest } from 'ember-qunit';
import { TestContext } from 'ember-test-helpers';
import { IBaseTrackingEvent } from '@datahub/shared/types/tracking/event-tracking';
import { TrackingEventCategory, TrackingGoal } from '@datahub/shared/constants/tracking/event-tracking';
import { getPiwikActivityQueue } from '@datahub/shared/utils/tracking/piwik';
import UnifiedTracking from '@datahub/shared/services/unified-tracking';
import Metrics from 'ember-metrics';
import MetricsServiceStub from '../../stubs/services/metrics';

module('Unit | Service | tracking', function(hooks): void {
  setupTest(hooks);

  hooks.beforeEach(function(this: TestContext) {
    this.owner.register('service:metrics', MetricsServiceStub);
  });

  test('service exists and is registered', function(assert): void {
    assert.ok(this.owner.lookup('service:unified-tracking'));
  });

  test('service methods proxy to metrics service and display expected behavior', function(assert): void {
    assert.expect(2);
    const trackingService: UnifiedTracking = this.owner.lookup('service:unified-tracking');
    const metricsService: Metrics = this.owner.lookup('service:metrics');
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
