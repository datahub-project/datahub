import { attribute } from '@ember-decorators/component';
import { computed } from '@ember/object';
import { reads } from '@ember/object/computed';
import { getAbsoluteUrl } from '@datahub/utils/helpers/url';
import DynamicLink from 'dynamic-link/components/dynamic-link';

/**
 * Defines the AnalyticsTrackableLinkTo class
 * Useful for automatically tracking the impressions and interactions of link content
 * Tracking must be initialized by invoking `trackAllContentImpressions` activity on paq
 * to track content
 * @export
 * @class AnalyticsTrackableLinkTo
 * @extends {LinkComponent}
 */
export default class AnalyticsTrackableLinkTo extends DynamicLink {
  /**
   * External handler for click events raised on this component instance
   */
  onClick?: (e: MouseEvent) => void;

  /**
   * Externally set attribute for grouping pieces of content.
   * Specifies the data attribute data-content-name derived from the component attribute,
   * contentName
   *
   * This defines the name of the content block to easily identify a specific block.
   * A content name groups different content pieces together
   * For instance, a content name could be "Product A", there could be many different content pieces
   * to exactly know which content was displayed and interacted with
   * @type {string}
   */
  @attribute('data-content-name')
  contentName?: string;

  /**
   * Externally set attribute for identifying this instance of content i.e. the actual content
   * that was displayed
   *
   * Multiple content pieces can be grouped together in a content name
   * @type {string}
   */
  @attribute('data-content-piece')
  contentPiece?: string;

  /**
   * Aliases the href property on the LinkComponent
   * @type {string}
   */
  @reads('href')
  hrefAlias!: string;

  /**
   * Specifies the data attribute data-content-target defining the target from interacting with this content block.
   * Useful to track the direction or path a user will take in navigating with this content
   * @readonly
   * @type {string}
   */
  @attribute('data-content-target')
  @computed('hrefAlias')
  get contentTarget(): string {
    return getAbsoluteUrl(this.hrefAlias);
  }

  /**
   * This specifies the data attribute data-track-content which defines a content block.
   * A content block is required for any content that is intended to be tracked
   * This attribute does not require any value hence intentionally narrow type of empty string ''
   * @type {''}
   */
  @attribute('data-track-content')
  readonly dataTrackContent: '' = '';

  /**
   * Invokes the the closure handler on click
   * @param {MouseEvent} e DOM mouse click event
   */
  click(e: MouseEvent): boolean {
    this.onClick && this.onClick(e);

    return super.click(e);
  }
}
