import Component from '@glimmer/component';

export interface IDynamicComponentsTextArgs {
  options: {
    // Any plain text string to render
    text: string;
  };
}

/**
 * The purpose of the dynamic text component is to be able to render simple text in our generic
 * rendering logic, for example, when using render props.
 *
 * @example
 * // In render props:
 * {
 *   name: 'dynamic-components/text'
 *   options: { text: 'Hello darkness my old friend' }
 * }
 */
export default class DynamicComponentsText extends Component<IDynamicComponentsTextArgs> {}
