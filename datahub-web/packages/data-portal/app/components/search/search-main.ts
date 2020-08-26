import Component from '@ember/component';
import { IEntityRenderCommonPropsSearch } from '@datahub/data-models/types/search/search-entity-render-prop';
import { computed } from '@ember/object';

/**
 * Will wrap all the search structure (container + presentation) so it is
 * easier to reuse the search look and feel (for example in XXX I Own)
 */
export default class SearchSearchMain extends Component {
  /**
   * Config for search
   */
  searchConfig: IEntityRenderCommonPropsSearch;

  /**
   * Computed flag determines if we show facets or not within search.
   * If flag showFacets not specified in config, `true` will be used.
   */
  @computed('searchConfig.showFacets')
  get showFacets(): boolean {
    const { showFacets } = this.searchConfig;
    // By default we will show facets
    return typeof showFacets === 'undefined' ? true : showFacets;
  }
}
