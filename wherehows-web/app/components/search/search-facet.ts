import Component from '@ember/component';
import { computed } from '@ember-decorators/object';
import { IFacetSelections } from 'wherehows-web/utils/api/search';

/**
 * Presentation component of a facet
 */
export default class SearchFacet extends Component {
  /**
   * Facet selections
   */
  selections: IFacetSelections;

  /**
   * Computed property to check if there is any selection in the
   * facet. If that is the case, a clear button will show up.
   */
  @computed('selections')
  get showClear(): boolean {
    const selections = this.selections || {};
    return Object.keys(selections).reduce((willShowClear: boolean, selectionKey: string) => {
      return willShowClear || selections[selectionKey];
    }, false);
  }
}
