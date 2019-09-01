import Component from '@ember/component';
import { get, getProperties } from '@ember/object';
import { action } from '@ember/object';
import Metrics from 'ember-metrics';
import { TrackingEventCategory } from 'wherehows-web/constants/analytics/event-tracking';
import { IBaseTrackingEvent } from 'wherehows-web/typings/app/analytics/event-tracking';
import { inject as service } from '@ember/service';

export default class TrackUiEvent extends Component.extend({
  tagName: '' //Creates a fragment component that will not have a DOM representation
}) {
  /**
   * References the metrics service
   * @type {ComputedProperty<Metrics>}
   */
  @service
  metrics: Metrics;

  /**
   * The set category for the event to be tracked
   * @type {TrackingEventCategory}
   */
  category: TrackingEventCategory;

  /**
   * The event to track
   */
  action: string;

  /**
   * An optional name of the event
   * @type {string}
   */
  name?: string;

  /**
   * An optional numeric value for the event
   * @type {number}
   */
  value?: number;

  /**
   * Invokes the metrics trackEvent service with options for the event being tracked
   * @memberof TrackUiEvent
   */
  _trackEvent(this: TrackUiEvent): void {
    const metrics = get(this, 'metrics');
    const { category, action, name, value }: IBaseTrackingEvent = getProperties(this, [
      'category',
      'action',
      'name',
      'value'
    ]);
    const resolvedOptions = Object.assign({}, { category, action }, !!name && { name }, !!value && { value });

    metrics.trackEvent(resolvedOptions);
  }

  /**
   * Passthrough action tracks an action triggered on a contained DOM component
   * @param {(...args: Array<any>) => any} uiAction the component action to passthrough
   * @param {...Array<any>} actionArgs args to be supplied to the ui component
   * @returns {void}
   * @memberof TrackUiEvent
   */
  @action
  trackActionAndPassthrough(uiAction: (...args: Array<any>) => any, ...actionArgs: Array<any>): void {
    uiAction(...actionArgs);
    return this._trackEvent();
  }
}
