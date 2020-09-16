import Component from '@glimmer/component';
import { ICustomSearchResultPropertyComponentTag } from '@datahub/data-models/types/search/custom-search-result-property-component/tag';

export default class CustomSearchResultPropertyComponentSearchTag extends Component<{
  /**
   * value can have the string which to show or a boolean to say whether we should show this tag
   * in this case, options.text must be defined
   */
  value?: string | boolean;

  /**
   * Options that this component accept
   */
  options?: ICustomSearchResultPropertyComponentTag['options'];
}> {
  baseClass = 'entity-pill';

  /**
   * returns the text to display for this tag.
   * text can come from the value or an option in options
   */
  get text(): string | undefined {
    const { value, options } = this.args;
    if (typeof value === 'boolean' && value && options && options.text) {
      return options.text;
    }
    if (typeof value === 'string' && value.length > 0) {
      return value;
    }

    return undefined;
  }
}
