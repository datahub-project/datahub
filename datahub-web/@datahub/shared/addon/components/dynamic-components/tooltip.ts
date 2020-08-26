import Component from '@glimmer/component';
import { IDynamicComponent } from '@datahub/shared/types/dynamic-component';

export interface IDynamicComponentsTooltipArgs {
  options: {
    // The component that will be placed in the trigger for the tooltip
    triggerComponent: IDynamicComponent;
    // The event (in accordance with ember-tooltips) that will trigger the tooltip content
    triggerOn?: 'click' | 'hover' | 'focus';
    // Any custom class(es) to add to the tooltip component itself
    className?: string;
    // The component(s) that we want to display in the tooltip content
    contentComponents: Array<IDynamicComponent>;
  };
}

/**
 * The purpose of the dynamic tooltip component is to be able to render a tooltip in our generic
 * rendering logic, for example, when using render props.
 *
 * @example
 * // In render props:
 * {
 *   name: 'dynamic-components/tooltip'
 *   options: {
 *     triggerComponent: {
 *       name: 'dynamic-components/icon',
 *       options: { icon: 'question-circle', prefix: 'far' }
 *     },
 *     triggerOn: 'hover',
 *     contentComponents: [
 *       { name: 'dynamic-component/text', options: { text: 'Click here to learn more!' } },
 *       {
 *         name: 'dynamic-component/wiki-link',
 *         options: { wikiKey: 'appHelp' }
 *        }
 *     ]
 *   }
 * }
 */
export default class DynamicComponentsTooltip extends Component<IDynamicComponentsTooltipArgs> {}
