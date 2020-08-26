import Component from '@glimmer/component';
import { IDynamicComponent } from '@datahub/shared/types/dynamic-component';

const baseStylingClassname = 'dynamic-header';

/**
 * Interface showcasing the type of arguments that are supplied to this component
 */
export interface IDynamicComponentsHeaderArgs {
  options: {
    // Styling class provided by consuming component
    classname?: string;
    // List of core content components
    contentComponents: Array<IDynamicComponent>;
    // Title text of the header
    title?: string;
  };
}

/**
 * The purpose of the dynamic header component is to be able to render headers in our generic
 * rendering logic, for example, when using render props.
 *
 * @example
 *  In render props:
 * {
 *   name: 'dynamic-components/header'
 *   options: {
 *      classname: 'charmander',
 *      title: 'firePokemon',
 *      contentComponents: [
 *        {
 *          name: 'dynamic-components/tooltip',
 *          options : {
 *          triggerOn: 'pokeBall',
 *          contentComponents: [
 *            {
 *              name: 'dynamic-components/wiki=link'
 *              options: { .......}
 *            }
 *          ]
 *        }
 *      ]
 *   }
 * }
 *
 */
export default class DynamicComponentsHeader extends Component<IDynamicComponentsHeaderArgs> {
  baseStylingClassname = baseStylingClassname;

  /**
   * Returns a dynamic text component used for the title of the header
   */
  get dynamicTextComponent(): IDynamicComponent {
    return {
      name: 'dynamic-components/text',
      options: {
        text: this.args.options.title || ''
      }
    };
  }
}
