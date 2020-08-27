import Component from '@glimmer/component';
import { action } from '@ember/object';
import Ember from 'ember';

/**
 * Params for this component
 */
interface ILinkToSectionArgs {
  // DOM selector of the element we want to scroll to.
  sectionSelector: string;
  // user defined CSS base class for customization
  baseClass?: string;
}

/**
 * Will calculate the height of all fixed navbars in DataHub. If one of the navbars is not found it will add 0
 * to the total height.
 */
const calculateHeaderOffsets = (): number => {
  const selectors = ['.banner-alert', '.navbar', '.nacho-breadcrumbs-container', '.entity-tabs'];
  return selectors.reduce(
    (offset: number, selector: string): number =>
      offset + (document.querySelector(selector)?.getBoundingClientRect().height || 0),
    0
  );
};

/**
 * This component will create a button which action will scroll to a certain element in the dom.
 * To select that element, you must pass a selector.
 */
export default class LinkToSection extends Component<ILinkToSectionArgs> {
  /**
   * Handler when button is clicked on the UI. It will scroll to a position,
   * determine by property top from an element + pageYOffset (in case there is some scroll already),
   * - headerOffset, which is needed as there might be some fixed navbars on top of the page.
   * @param sectionSelector dom selector for the element you want to scroll
   */
  @action
  scrollTo(sectionSelector: string): void {
    const element = document.querySelector(sectionSelector);
    const scrollContainer = Ember.testing ? document.querySelector('#ember-testing-container') : window;
    // The testing page itself can have pageYOffset too
    const pageYOffset = Ember.testing
      ? -(document.querySelector('#ember-testing')?.getBoundingClientRect().top || 0)
      : window.pageYOffset;

    if (element && scrollContainer) {
      const headerOffset = calculateHeaderOffsets();
      const boundingRect = element.getBoundingClientRect();
      const absolutePosition = boundingRect.top + pageYOffset;
      const offsetPosition = absolutePosition - headerOffset;

      scrollContainer.scrollTo({
        top: offsetPosition,
        behavior: 'smooth'
      });
    }
  }
}
