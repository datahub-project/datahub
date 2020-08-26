import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../templates/components/custom-search-result-property-component/date';
import { computed } from '@ember/object';
import { layout, tagName } from '@ember-decorators/component';
import { ICustomSearchResultPropertyComponentDate } from '@datahub/data-models/types/search/custom-search-result-property-component/date';

/**
 * Component to render field as formatted dates.
 */
@tagName('')
@layout(template)
export default class CustomSearchResultPropertyComponentDate extends Component {
  /**
   * number value of the date
   */
  value?: number;

  /**
   * options passed from search should specify format
   */
  options?: ICustomSearchResultPropertyComponentDate['options'];

  /**
   * Sometimes BE returns time in seconds instead of MS.
   * this will transform the date to something moment will accept
   */
  @computed('value')
  get fixedValue(): number | undefined {
    const { options, value } = this;
    if (options && typeof value !== 'undefined' && options.inSeconds) {
      return value * 1000;
    }
    return value;
  }
}
