import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../templates/components/truncated-text-with-tooltip';
import { layout, tagName } from '@ember-decorators/component';
import { IDynamicLinkParams } from 'dynamic-link/components/dynamic-link';
import { action, set, computed } from '@ember/object';

/**
 * Default base class for the component
 */
const _baseClass = 'truncated-text-with-tooltip';

/**
 * This component renders a dynamic link or plain text based on optional params
 * and truncates the text with ellipsis and a tooltip with the full text if it overflows
 * Parent component must render this component with a set width for overflow detection
 */
@layout(template)
@tagName('')
export default class TruncatedTextWithTooltip extends Component {
  /**
   * Text displayed on both the component and the tooltip
   */
  text = '';

  /**
   * Optional class names that can be passed into the component
   */
  className?: string = '';

  /**
   * TODO META-10151: Refactor truncated-text-with-tooltip tooltip logic to default hidden
   * Flag that represents whether the component needs a tooltip
   * Based on whether or not the text will overflow the cell
   */
  showTooltip = true;

  /**
   * If link params are passed in the component will render a dynamic link
   * Otherwise, the component will render the text in a span
   */
  linkParams?: IDynamicLinkParams;

  /**
   * Used as the base class of the component
   * If class names were passed in, then appends to the default base class
   */
  @computed('className')
  get baseClass(): string {
    const { className } = this;
    return className ? `${_baseClass} ${className}` : _baseClass;
  }

  /**
   * On hover, calculates and sets showTooltip property to false
   * if the title is short, untruncated, and does not require a tooltip
   * @param {MouseEvent} e received from the event listener for onmouseover attached to text component
   */
  @action
  hideTooltipIfUntruncated(e: MouseEvent): void {
    const target = e.target as HTMLElement | null;
    if (this.showTooltip && target && target.scrollWidth <= target.offsetWidth) {
      set(this, 'showTooltip', false);
    }
  }
}
