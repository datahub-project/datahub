import Component from '@ember/component';
import { readSuggestions } from 'wherehows-web/utils/api/search/suggestions';
import { RouterService } from 'ember';
import { service } from '@ember-decorators/service';
import { task, timeout } from 'ember-concurrency';
import { computed } from '@ember-decorators/object';
import SearchService from 'wherehows-web/services/search';
import { ISuggestionsResponse } from 'wherehows-web/typings/app/search/suggestions';
import { isEmpty } from '@ember/utils';

/**
 * Runtime cache of recently seen typeahead results
 * @type {Object.<Object>} a hash of urls to results
 */
const keywordResultsCache: Record<string, Array<string>> = {};

/**
 * Search box container that handle all the data part for
 * the search box
 */
export default class SearchBoxContainer extends Component {
  @service
  router: RouterService;

  @service
  search: SearchService;

  /**
   * Placeholder for input
   */
  placeholder: string = 'Search datasets by keywords... e.g. pagekey';

  /**
   * Search route will update the service with current keywords
   * since url will contain keyword
   */
  @computed('search.keyword')
  get keyword(): string {
    return this.search.keyword;
  }

  /**
   * Suggestions handle. Will debounce, cache suggestions requests
   * @param  {string} text suggestion for this text
   */
  onUserType = task(function*(this: SearchBoxContainer, text: string) {
    if (text.length > 2) {
      const cachedKeywords = keywordResultsCache[String(text)];
      if (!isEmpty(cachedKeywords)) {
        return yield cachedKeywords;
      }
      yield timeout(200);
      const response: ISuggestionsResponse = yield readSuggestions({ input: text });
      keywordResultsCache[String(text)] = response.source;
      return response.source;
    } else {
      return yield [];
    }
  }).restartable();

  /**
   * When search actually happens, then we transition to a new route.
   * @param text search term
   */
  onSearch(text: string) {
    this.router.transitionTo('search', {
      queryParams: {
        keyword: text,
        category: 'datasets',
        page: 1,
        facets: ''
      }
    });
  }
}
