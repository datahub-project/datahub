import LinkComponent from '@ember/routing/link-component';
import { attribute } from '@ember-decorators/component';
import { computed } from '@ember/object';
import { reads } from '@ember/object/computed';
import { assert } from '@ember/debug';
import { getAbsoluteUrl } from 'wherehows-web/utils/helpers/url';

/**
 * Defines the AnalyticsTrackableLinkTo class
 * Useful for automatically tracking the impressions and interactions of link content
 * Tracking must be initialized by invoking `trackAllContentImpressions` activity on paq
 * to track content
 * @export
 * @class AnalyticsTrackableLinkTo
 * @extends {LinkComponent}
 */
export default class AnalyticsTrackableLinkTo extends LinkComponent {
  /**
   * External handler for click events raised on this component instance
   * @memberof AnalyticsTrackableLinkTo
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
   * @memberof AnalyticsTrackableLinkTo
   */
  @attribute('data-content-name')
  contentName!: string;

  /**
   * Externally set attribute for identifying this instance of content i.e. the actual content
   * that was displayed
   *
   * Multiple content pieces can be grouped together in a content name
   * @type {string}
   * @memberof AnalyticsTrackableLinkTo
   */
  @attribute('data-content-piece')
  contentPiece!: string;

  /**
   * Aliases the href property on the LinkComponent
   * @type {string}
   * @memberof AnalyticsTrackableLinkTo
   */
  @reads('href')
  hrefAlias: string;

  /**
   * Specifies the data attribute data-content-target defining the target from interacting with this content block.
   * Useful to track the direction or path a user will take in navigating with this content
   * @readonly
   * @type {string}
   * @memberof AnalyticsTrackableLinkTo
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
   * @memberof AnalyticsTrackableLinkTo
   */
  @attribute('data-track-content')
  readonly dataTrackContent: '' = '';

  init() {
    super.init();
    const { contentName, contentPiece } = this;

    assert(`Expected contentName to be of type string`, typeof contentName === 'string');
    assert(`Expected contentPiece to be of type string`, typeof contentPiece === 'string');
  }

  /**
   * Invokes the the closure handler on click
   * @param {MouseEvent} e DOM mouse click event
   * @memberof AnalyticsTrackableLinkTo
   */
  click(e: MouseEvent): void {
    this.onClick && this.onClick(e);
  }
}
