import Controller from '@ember/controller';
import { computed } from '@ember-decorators/object';
import { debounce } from '@ember-decorators/runloop';
import {
  IFacetsSelectionsMap,
  facetToParamUrl,
  facetFromParamUrl,
  facetToDynamicCounts,
  IFacetsCounts
} from 'wherehows-web/utils/api/search';

// gradual refactor into es class, hence extends EmberObject instance
export default class SearchController extends Controller {
  queryParams = ['category', 'page', 'facets', 'keyword'];

  /**
   * The category to narrow/ filter search results
   * @type {string}
   */
  category = 'datasets';

  /**
   * Encoded facets state in a restli fashion
   */
  facets: string;

  /**
   * The current search page
   * @type {number}
   */
  page = 1;

  /**
   * Header text for search sidebar
   * @type {string}
   */
  header = 'Refine By';

  /**
   * Since the loading of search comes from two parts:
   * 1. Search Call
   * 2. During debouncing
   *
   * We put it as a flag to control it better
   */
  searchLoading: boolean = false;

  /**
   * When facets change we set the flag loading and call the debounced fn
   * @param selections facet selections
   */
  onFacetsChange(selections: IFacetsSelectionsMap) {
    this.set('searchLoading', true);
    this.onFacetsChangeDebounced(selections);
  }

  /**
   * Will set the facets in the URL to start a model refresh (see route)
   * @param selections Facet selections
   */
  @debounce(1000)
  onFacetsChangeDebounced(selections: IFacetsSelectionsMap) {
    this.setProperties({
      facets: facetToParamUrl(selections),
      page: 1
    });
  }

  /**
   * Will translate backend fields into a dynamic facet
   * count structure.
   */
  @computed('model')
  get facetCounts(): IFacetsCounts {
    return facetToDynamicCounts(this.model);
  }

  /**
   * Will read selections from URL and translate it into
   * our selections object
   */
  @computed('facets')
  get facetsSelections(): IFacetsSelectionsMap {
    return facetFromParamUrl(this.facets || '');
  }

  /**
   * Will return false if there is data to display
   */
  @computed('model.data.length')
  get showNoResult() {
    return this.model.data ? this.model.data.length === 0 : true;
  }

  @computed('model.category')
  get isMetrics(): boolean {
    const model = this.get('model');
    if (model && model.category) {
      if (model.category.toLocaleLowerCase() === 'metrics') {
        return true;
      }
    }
    return false;
  }

  @computed('model.page')
  get previousPage(): number {
    const model = this.get('model');
    if (model && model.page) {
      var currentPage = model.page;
      if (currentPage <= 1) {
        return currentPage;
      } else {
        return currentPage - 1;
      }
    } else {
      return 1;
    }
  }

  @computed('model.page')
  get nextPage(): number {
    const model = this.get('model');
    if (model && model.page) {
      const currentPage = model.page;
      const totalPages = model.totalPages;
      if (currentPage >= totalPages) {
        return totalPages;
      } else {
        return currentPage + 1;
      }
    } else {
      return 1;
    }
  }

  @computed('model.page')
  get first(): boolean {
    const model = this.get('model');
    if (model && model.page) {
      const currentPage = model.page;
      if (currentPage <= 1) {
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  @computed('model.page')
  get last(): boolean {
    const model = this.get('model');
    if (model && model.page) {
      const currentPage = model.page;
      const totalPages = model.totalPages;
      if (currentPage >= totalPages) {
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }
}
