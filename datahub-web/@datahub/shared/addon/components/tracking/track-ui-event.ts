import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../templates/components/tracking/track-ui-event';
import { layout, tagName } from '@ember-decorators/component';
import { inject as service } from '@ember/service';
import UnifiedTracking from '@datahub/shared/services/unified-tracking';
import { TrackingEventCategory } from '@datahub/shared/constants/tracking/event-tracking';
import { IBaseTrackingEvent } from '@datahub/shared/types/tracking/event-tracking';
import { action } from '@ember/object';
import { noop } from 'lodash';

/**
 * Tag-less component / fragment to track user interactions or actions from an hbs template. Removes the need to concern
 * component logic with analytics operations, which can evolve independently
 * @export
 * @class TrackUiEvent
 * @extends {Component}
 */
@layout(template)
@tagName('')
export default class TrackUiEvent extends Component {
  /**
   * Reference to the UnifiedTracking tracking service for analytics tracking within host application
   */
  @service('unified-tracking')
  tracking!: UnifiedTracking;

  /**
   * Reference to the enum of tracking event categories
   */
  category?: TrackingEventCategory;

  /**
   * The action that was performed for the event category
   */
  action?: IBaseTrackingEvent['action'];

  /**
   * The name of the tracking event
   */
  name?: IBaseTrackingEvent['name'];

  /**
   * An optional value for the specific event being tracked
   */
  value?: IBaseTrackingEvent['value'];

  /**
   * Invokes the UnifiedTracking service with options for the specific event being tracked
   * @private
   */
  private trackEvent(): void {
    const { category, action, name, value }: Partial<IBaseTrackingEvent> = this;

    if (category && action) {
      const resolvedOptions = Object.assign({}, { category, action }, !!name && { name }, !!value && { value });
      this.tracking.trackEvent(resolvedOptions);
    }
  }

  /**
   * Tracks an action triggered on a nested component
   * @param {(...args: Array<unknown>) => unknown} uiAction the action handler intended to handle the actual user interaction
   * @param {...Array<unknown>} actionArgs arguments intended to be supplied to the actual action handler
   */
  @action
  trackOnAction(uiAction: (...args: Array<unknown>) => unknown = noop, ...actionArgs: Array<unknown>): void {
    typeof uiAction === 'function' && uiAction(...actionArgs);
    this.trackEvent();
  }
}
