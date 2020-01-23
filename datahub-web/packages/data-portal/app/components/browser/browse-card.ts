import Component from '@ember/component';
import { IDynamicLinkNode } from '@datahub/utils/types/vendor/dynamic-link';

/**
 * Card component. It is used in the home page of the app to
 * show the different types of entity
 */
export default class BrowseCard extends Component {
  // Title of the card
  title: string;
  // Description below the title
  description: string;
  // It will show a pendulum animation when true embedded in the card
  showSpinner: boolean;
  // Target link for the user when it clicks the card
  link: IDynamicLinkNode<unknown, unknown>;
}
