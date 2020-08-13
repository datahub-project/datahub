import Component from '@ember/component';
import { IDynamicLinkNode } from '@datahub/utils/types/vendor/dynamic-link';
import { noop } from 'lodash';
import { action } from '@ember/object';
// @ts-ignore: Ignore import of compiled template
import template from '../../templates/components/browser/browse-card';
import { layout } from '@ember-decorators/component';

/**
 * Card component. It is used in the home page of the app to
 * show the different types of entity
 */
@layout(template)
export default class BrowseCard extends Component {
  // Title of the card
  title?: string;
  // Description below the title
  description?: string;
  // It will show a pendulum animation when true embedded in the card
  showSpinner?: boolean;
  // Target link for the user when it clicks the card
  link?: IDynamicLinkNode<unknown, unknown>;

  /**
   * Externally supplied reference to the host components click handler
   */
  didClickCard: (e: string) => void = noop;

  /**
   * Handler to pass the current card name to the external event handler
   */
  @action
  onClick(): void {
    const { didClickCard, title } = this;

    didClickCard && title && didClickCard(title);
  }
}
