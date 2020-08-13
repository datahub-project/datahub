import CurrentUser from '@datahub/shared/services/current-user';
import { action } from '@ember/object';
import Component from '@ember/component';
import { inject as service } from '@ember/service';
import UnifiedTracking from '@datahub/shared/services/unified-tracking';
import {
  TrackingEventCategory,
  CustomTrackingEventName,
  PageKey
} from '@datahub/shared/constants/tracking/event-tracking';
import {
  ICategoryContainerTrackEventParams,
  IBaseTrackingEvent,
  ICustomEventData
} from '@datahub/shared/types/tracking/event-tracking';
// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/browser/containers/category-container';
import { layout } from '@ember-decorators/component';

@layout(template)
export default class BrowserContainersCategoryContainer extends Component {
  /**
   * References the application service to track user events, metrics and interaction
   */
  @service('unified-tracking')
  tracking!: UnifiedTracking;

  /**
   * Injects the application CurrentUser service to provide the actor information on the action event
   */
  @service('current-user')
  sessionUser!: CurrentUser;

  /**
   * Click handler constructs a custom event of type `DataHubBrowseActionEvent` and hands off the
   * customEventData to the tracking service
   * @param {string} categoryName the name of the browse category for the clicked card
   */
  @action
  onCardClick(categoryName: string): void {
    const userName = this.sessionUser.entity?.username || '';
    this.trackBrowseCategoryEvent({ userName, categoryName });
  }

  /**
   * Creates attributes for tracking the browse action event and  invokes the tracking service
   * @param {ICategoryContainerTrackEventParams} { userName, title } the current user's username and the title of the card
   */
  trackBrowseCategoryEvent({ userName, categoryName }: ICategoryContainerTrackEventParams): void {
    const baseEventAttrs: IBaseTrackingEvent = {
      category: TrackingEventCategory.Entity,
      action: CustomTrackingEventName.BrowseAction
    };
    const customEventAttrs: ICustomEventData = {
      pageKey: PageKey.browseCategory,
      eventName: CustomTrackingEventName.BrowseAction,
      userName: userName,
      target: categoryName,
      body: {
        actorUrn: userName,
        targetUrn: categoryName
      }
    };

    this.tracking.trackEvent(baseEventAttrs, customEventAttrs);
  }
}
