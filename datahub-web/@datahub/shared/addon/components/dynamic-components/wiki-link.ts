import Component from '@glimmer/component';
import { inject as service } from '@ember/service';
import { IConfigurator } from '@datahub/shared/types/configurator/configurator';

export interface IDynamicComponentsWikiLinkArgs {
  options: {
    // The property in the wikiLinks object in the configs that gets us the link we want specifically
    wikiKey: string;
    // Any text we want to show as the link text (as opposed to the link itself being shown to the user)
    text?: string;
    // Any custom class(es) we want to show
    className?: string;
  };
}

/**
 * The purpose of the dynamic wiki link component is to be able to render a link to our wiki pages
 * (a mapping of which exists in the configurator) in our generic rendering logic, for example,
 * when using render props.
 *
 * @example
 * // In render props:
 * {
 *   name: 'dynamic-components/wiki-link'
 *   options: { wikiKey: 'appHelp', text: 'Learn more' }
 * }
 */
export default class DynamicComponentsWikiLink extends Component<IDynamicComponentsWikiLinkArgs> {
  /**
   * Injects the configurator service so that we can access the mapping of wiki links we received
   * from our configs
   */
  @service
  configurator!: IConfigurator;

  /**
   * Returns the wiki link from the configs specified by the key from the options argument
   */
  get wikiLink(): string {
    const wikiKey = this.args.options.wikiKey;
    return this.configurator.getConfig('wikiLinks')[wikiKey];
  }
}
