import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../templates/components/custom-search-result-property-component/tag';
import { layout, tagName } from '@ember-decorators/component';
import { ICustomSearchResultPropertyComponentTag } from '@datahub/data-models/types/search/custom-search-result-property-component/tag';

/**
 * This is a wrapper for nacho-pill that accepts options
 */
@layout(template)
@tagName('')
export default class CustomSearchResultPropertyComponentSearchTag extends Component {
  baseClass = 'entity-pill';

  /**
   * value can have the string which to show or a boolean to say whether we should show this tag
   * in this case, options.text must be defined
   */
  value?: string | boolean;

  /**
   * returns the text to display for this tag.
   * text can come from the value or an option in options
   */
  get text(): string | undefined {
    const { value, options } = this;
    if (typeof this.value === 'boolean' && value && options && options.text) {
      return options.text;
    }
    if (typeof value === 'string' && value.length > 0) {
      return value;
    }

    return undefined;
  }

  /**
   * Options that this component accept
   */
  options?: ICustomSearchResultPropertyComponentTag['options'];
}
