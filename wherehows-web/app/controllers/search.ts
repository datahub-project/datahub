import Controller from '@ember/controller';
import { computed } from '@ember-decorators/object';
import { debounce } from '@ember-decorators/runloop';
import { IFacetsSelectionsMap, facetToParamUrl, facetFromParamUrl } from 'wherehows-web/utils/api/search';

// gradual refactor into es class, hence extends EmberObject instance
export default class SearchController extends Controller {
  queryParams = ['category', 'page', 'facets', 'keyword'];

  /**
   * The category to narrow/ filter search results
   * @type {string}
   */
  category = 'datasets';

  /**
   * Dataset Platform to restrict search results to
   * @type {DatasetPlatform}
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

  searchLoading: boolean = false;

  onFacetsChange(selections: IFacetsSelectionsMap) {
    this.set('searchLoading', true);
    this.onFacetsChangeDebounced(selections);
  }

  @debounce(1000)
  onFacetsChangeDebounced(selections: IFacetsSelectionsMap) {
    this.setProperties({
      facets: facetToParamUrl(selections),
      page: 1
    });
  }

  @computed('facets')
  get facetsSelections() {
    return facetFromParamUrl(this.facets || '');
  }
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
