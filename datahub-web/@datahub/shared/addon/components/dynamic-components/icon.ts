import Component from '@glimmer/component';

export interface IDynamicComponentsIconArgs {
  options: {
    // Name of the icon (in accordance with FaIcon component from ember-fontawesome)
    icon: string;
    // Any custom class(es) we want to add to the icon
    className?: string;
    // In accordance with ember-fontawesome, we may need a prefix to render a specific icon properly
    prefix?: string;
  };
}

/**
 * The purpose of the dynamic icon component is to be able to render a font awesome icon in our
 * generic rendering logic, for example, when using render props.
 *
 * @example
 * // In render props:
 * {
 *   name: 'dynamic-components/icon'
 *   options: { icon: 'question-circle', prefix: 'far' }
 * }
 */
export default class DynamicComponentsIcon extends Component<IDynamicComponentsIconArgs> {}
