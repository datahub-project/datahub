import Component from '@glimmer/component';
import { IDynamicComponentArgs } from '@datahub/shared/types/dynamic-component';
import { IDynamicComponentsTooltipArgs } from '@datahub/shared/components/dynamic-components/tooltip';
import { IDynamicComponentsTextArgs } from '@datahub/shared/components/dynamic-components/text';
import { IDynamicComponentsWikiLinkArgs } from '@datahub/shared/components/dynamic-components/wiki-link';

export interface IHelpTooltipWithLinkArgs {
  options: {
    // The text to show up in the help tooltip
    text?: string;
    // A key for the wiki link (if we want one)
    wikiKey?: string;
    // The text for the wiki link (if we want one)
    wikiLinkText?: string;
    // Trigger on a certain user action
    triggerOn?: IDynamicComponentsTooltipArgs['options']['triggerOn'];
    // Custom classname for the tooltip
    className?: string;
  };
}

/**
 * The help tooltip with link component combines the dynamic components:
 * - tooltip
 * - icon
 * - text
 * - wiki link
 * In a common pattern that creates a help tooltip.
 * This is a shortcut that keeps the compatibility with generic render props but removes a bit of the
 * overhead to actually create such a component
 *
 * @example
 * renderProps = {
 *   options: {
 *     text: 'Pikachu is an electric pokemon',
 *     wikiKey: 'pikachu',
 *     wikiLinkText: 'Learn more',
 *     triggerOn: 'hover'
 *   }
 * }
 */
export default class HelpTooltipWithLink extends Component<IHelpTooltipWithLinkArgs> {
  get tooltipOptions(): IDynamicComponentsTooltipArgs['options'] {
    const { text, wikiKey, wikiLinkText, triggerOn } = this.args.options;
    const tooltipContent: Array<IDynamicComponentArgs<
      IDynamicComponentsTextArgs | IDynamicComponentsWikiLinkArgs
    >> = [];

    if (text) {
      const textContent: IDynamicComponentArgs<IDynamicComponentsTextArgs> = {
        name: 'dynamic-components/text',
        options: { text }
      };
      tooltipContent.push(textContent);
    }

    if (wikiKey) {
      const wikiLinkContent: IDynamicComponentArgs<IDynamicComponentsWikiLinkArgs> = {
        name: 'dynamic-components/wiki-link',
        options: { wikiKey, text: wikiLinkText }
      };
      tooltipContent.push(wikiLinkContent);
    }

    return {
      // While the tooltip default is click, we default to hover here since that is the more common
      // behavior for help tooltips
      triggerOn: triggerOn || 'hover',
      triggerComponent: {
        name: 'dynamic-components/icon',
        options: { icon: 'question-circle', prefix: 'far', className: 'dynamic-tooltip__inline-trigger-icon' }
      },
      contentComponents: tooltipContent
    };
  }
}
