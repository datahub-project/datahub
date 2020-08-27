import Modifier from 'ember-modifier';
import { inject as service } from '@ember/service';
import { action } from '@ember/object';
import { ControlInteractionType } from '@datahub/shared/constants/tracking/event-tracking/control-interaction-events';
import UnifiedTracking from '@datahub/shared/services/unified-tracking';
import { TrackingEventCategory } from '@datahub/shared/constants/tracking/event-tracking';
import { IBaseTrackingEvent } from '@datahub/shared/types/tracking/event-tracking';

interface ITrackControlInteractionArgs {
  named: {
    // The type of control interaction we want to track with this component
    type?: ControlInteractionType;
    // Name of the control, intended to identify this type of control
    name: string;
  };
}

/**
 * The TrackControlInteraction modifier can be attached to an element in order to trigger control
 * interaction events on that element based on what kind of interaction we are interested in
 *
 * Usage of this modifier expects 2 named arguments:
 * type - the type of control we want to track
 * name - the name of the control we want to use
 *
 * Check ControlInteractionType enum for the types we currently support
 * @example
 * <button {{track-control-interaction type="SHORT_PRESS" name="charmander"}}>Click me</button>
 */
export default class TrackControlInteraction extends Modifier<ITrackControlInteractionArgs> {
  /**
   * Maps a supported type of interaction to the event name that would trigger the tracking function
   */
  static mapControlInteractionTypeToTrigger: Record<ControlInteractionType, string> = {
    [ControlInteractionType.click]: 'click',
    [ControlInteractionType.focus]: 'focus',
    [ControlInteractionType.hover]: 'mouseenter'
  };

  /**
   * Gets our passed in interaction type, or defaults to being a "click" handler
   */
  get interactionType(): ControlInteractionType {
    return this.args.named?.type || ControlInteractionType.click;
  }

  /**
   * Shortcuts to our passed in control name
   */
  get controlName(): string {
    return this.args.named.name;
  }

  /**
   * Injection of our tracking service in order to actually fire the event once we have built it
   */
  @service('unified-tracking')
  tracking!: UnifiedTracking;

  /**
   * Builds out our tracking event parameters. This allows us to easily access what the open source
   * event tracking passes to the service's trackEvent() in an extended class from this one
   */
  protected buildTrackingEventParams(): IBaseTrackingEvent | void {
    const category = TrackingEventCategory.ControlInteraction;
    const action = this.controlName;

    return { category, action };
  }

  /**
   * Triggers the trackEvent() method from our service once we've built our tracking parameters upon
   * a user interaction
   */
  protected trackEvent(): void {
    const params = this.buildTrackingEventParams();
    params && this.tracking.trackEvent(params);
  }

  /**
   * Given the type of interaction we are working with, will return the correct "on" trigger to
   * fire the event
   */
  get eventTrigger(): string {
    return TrackControlInteraction.mapControlInteractionTypeToTrigger[this.interactionType];
  }

  /**
   * Context bound method to add to the event listener so that we can trigger an interaction
   * event upon the correct user action
   */
  @action
  onTriggerEventTracking(): void {
    this.trackEvent();
  }

  /**
   * Adds the event listener for our tracking event to the element
   */
  addEventListener(): void {
    this.element.addEventListener(this.eventTrigger, this.onTriggerEventTracking);
  }

  /**
   * Removes our event listener for our tracking event from the element
   */
  removeEventListener(): void {
    this.element.removeEventListener(this.eventTrigger, this.onTriggerEventTracking);
  }

  didReceiveArguments(): void {
    if (!this.args.named?.name) {
      throw new Error(
        `Modifier [tracking-control-interaction] for element: ${this.element.tagName} should have received a control name`
      );
    }
  }

  didInstall(): void {
    this.addEventListener();
  }

  willRemove(): void {
    this.removeEventListener();
  }

  constructor(...args: Array<unknown>) {
    super(...args);
  }
}
