import Component from '@glimmer/component';
import { computed } from '@ember/object';
import { ICustomSearchResultPropertyComponentDate } from '@datahub/data-models/types/search/custom-search-result-property-component/date';

/**
 * Component to render field as formatted dates.
 */
export default class CustomSearchResultPropertyComponentDate extends Component<{
  /**
   * number value of the date
   */
  value?: number;

  /**
   * options passed from search should specify format
   */
  options?: ICustomSearchResultPropertyComponentDate['options'];
}> {
  /**
   * Sometimes BE returns time in seconds instead of MS.
   * this will transform the date to something moment will accept
   */
  @computed('args.value')
  get fixedValue(): number | undefined {
    const { options, value } = this.args;
    if (options && typeof value !== 'undefined' && options.inSeconds) {
      return value * 1000;
    }
    return value;
  }
}
