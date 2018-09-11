import Controller from '@ember/controller';
import { set } from '@ember/object';
import { action, computed } from '@ember-decorators/object';

// gradual refactor into es class, hence extends EmberObject instance
export default class SearchController extends Controller {
  queryParams = ['keyword', 'category', 'source', 'page'];

  /**
   * Search keyword to look for
   * @type {string}
   */
  keyword = '';

  /**
   * The category to narrow/ filter search results
   * @type {string}
   */
  category = 'datasets';

  /**
   * Dataset Platform to restrict search results to
   * @type {'all'|DatasetPlatform}
   */
  source = 'all';

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
   * Handles the response to changing the source platform to search through
   * @param source
   */
  @action
  sourceDidChange(source: string) {
    set(this, 'source', source);
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
