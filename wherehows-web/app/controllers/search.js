import Controller from '@ember/controller';
import { computed, set, get } from '@ember/object';
import { capitalize } from '@ember/string';
import { action } from '@ember-decorators/object';

// gradual refactor into es class, hence extends EmberObject instance
export default class Search extends Controller.extend({
  isMetrics: computed('model.category', function() {
    var model = this.get('model');
    if (model && model.category) {
      if (model.category.toLocaleLowerCase() === 'metrics') {
        return true;
      }
    }
    return false;
  }),

  previousPage: computed('model.page', function() {
    var model = this.get('model');
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
  }),
  nextPage: computed('model.page', function() {
    var model = this.get('model');
    if (model && model.page) {
      var currentPage = model.page;
      var totalPages = model.totalPages;
      if (currentPage >= totalPages) {
        return totalPages;
      } else {
        return currentPage + 1;
      }
    } else {
      return 1;
    }
  }),
  first: computed('model.page', function() {
    var model = this.get('model');
    if (model && model.page) {
      var currentPage = model.page;
      if (currentPage <= 1) {
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }),
  last: computed('model.page', function() {
    var model = this.get('model');
    if (model && model.page) {
      var currentPage = model.page;
      var totalPages = model.totalPages;
      if (currentPage >= totalPages) {
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  })
}) {
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
  sourceDidChange(source) {
    set(this, 'source', source);
  }
}
