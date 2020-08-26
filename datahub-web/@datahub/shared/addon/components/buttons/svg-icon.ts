import Component from '@glimmer/component';
import { noop } from 'lodash';
import { action } from '@ember/object';

interface IButtonsSvgIconArgs {
  // Alternate / tooltip text content
  title?: string;
  // An optional class directly targeting the svg image
  iconClass?: string;
  // Name of the svg icon to render within the context of the button
  // This is the file name for the svg image which should be in a directory path
  // that is accessible to svg-bar helper, typically @datahub/shared/public/assets/images/svgs/svg
  iconName?: string;
  // on click handler for handling click events from the button element
  onClick?: () => unknown;
}

// BEM Block class for the component
export const baseClass = 'svg-icon-button';

/**
 * Renders a button element with an svg image as the label
 * Not intended for a textual button but provides the option to render a tooltip when the title argument
 * is provided
 * @export
 * @class ButtonsSvgIcon
 * @extends {Component<IButtonsSvgIconArgs>}
 */
export default class ButtonsSvgIcon extends Component<IButtonsSvgIconArgs> {
  /**
   * Referenced in template
   */
  baseClass = baseClass;

  /**
   * Invokes the onclick handler if one is provided
   */
  @action
  onClick(): void {
    const { onClick = noop } = this.args;
    onClick();
  }
}
