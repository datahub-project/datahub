import Service from '@ember/service';
import Metrics from 'ember-metrics';
import { inject as service } from '@ember/service';
import { IBaseTrackingEvent, IBaseTrackingGoal } from 'wherehows-web/typings/app/analytics/event-tracking';
import { getPiwikActivityQueue } from 'wherehows-web/utils/analytics/piwik';

export default class TrackingService extends Service {
  /**
   * References the ember-metrics service for tracking events
   * @type {Metrics}
   * @memberof TrackingService
   */
  @service
  metrics: Metrics;

  /**
   * Track a raised event to report to tracking service
   * @param {IBaseTrackingEvent} event the interesting event to be tracked
   * @memberof TrackingService
   */
  trackEvent(event: IBaseTrackingEvent) {
    const { category, action, name, value } = event;
    const resolvedOptions = Object.assign({}, { category, action }, !!name && { name }, !!value && { value });

    this.metrics.trackEvent(resolvedOptions);
  }

  /**
   * Track when a goal is met
   * @param {IBaseTrackingGoal} goal the goal to be tracked
   * @memberof TrackingService
   */
  trackGoal(goal: IBaseTrackingGoal) {
    getPiwikActivityQueue().push(['trackGoal', goal.name]);
  }
}
