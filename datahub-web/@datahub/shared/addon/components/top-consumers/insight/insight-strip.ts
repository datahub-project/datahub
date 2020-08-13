import Component from '@glimmer/component';
import { noop } from 'lodash';
import { IDynamicLinkParams } from 'dynamic-link/components/dynamic-link';
import { TopConsumer } from '@datahub/metadata-types/constants/metadata/top-consumers';

interface ITopConsumersInsightStripArgs {
  /**
   * Title of the top consumer insight
   */
  title: string;

  /*
   * Tooltip text for this component to provide additional information
   */
  tooltipText: string;

  /**
   * A list of top consumer entity link params
   * Redirects the user to the entity page either on DataHub (if modeled) or externally
   */
  topConsumers: Array<IDynamicLinkParams | string>;

  /**
   * The type of consumer used by this consumer for rendering avatars or links
   */
  consumerType: TopConsumer;

  /**
   * Action triggered when the user clicks on the show more button
   */
  onClickShowMore?: () => void;
}

/**
 * The preview limit on the number of top consumers shown on this insight without viewing all
 */
export const PREVIEW_LIMIT = 2;

export const baseClass = 'top-consumers-insight-strip';

/**
 * This component is an insight strip that displays a preview of list of top consumers
 * (of the same consumer entity type) based on some preview limit.
 * The number of consumers not shown in the insight strip will be displayed to the user
 * and the user can access all top consumers information by clicking show more.
 */
export default class TopConsumersInsightStrip extends Component<ITopConsumersInsightStripArgs> {
  /**
   * Declared for convenient access in the template
   */
  baseClass = baseClass;

  /**
   * Top consumer enum declared in the component for convenient access
   */
  topConsumer = TopConsumer;

  /**
   * The preview top consumers computed from the preview limit
   */
  get topConsumersPreview(): Array<IDynamicLinkParams | string> {
    return this.args.topConsumers.slice(0, PREVIEW_LIMIT);
  }

  /**
   * The number of top consumers hidden from the preview
   */
  get numberOfTopConsumersHidden(): number {
    const listLength = this.args.topConsumers.length;

    return Math.max(listLength - PREVIEW_LIMIT, 0);
  }

  /**
   * Action triggered when the user clicks on the show more button
   */
  onClickShowMore: () => void = this.args.onClickShowMore || noop;
}
