import Component from '@ember/component';
import { readSuggestions } from 'wherehows-web/utils/api/search/suggestions';
import { RouterService } from 'ember';
import { service } from '@ember-decorators/service';
import { task, timeout } from 'ember-concurrency';
import SearchService from 'wherehows-web/services/search';
import { ISuggestionsResponse } from 'wherehows-web/typings/app/search/suggestions';
import { isEmpty } from '@ember/utils';
import { alias } from '@ember-decorators/object/computed';

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
  @alias('search.keyword')
  keyword: string;

  /**
   * Suggestions handle. Will debounce, cache suggestions requests
   * @param  {string} text suggestion for this text
   */
  onTypeahead = task<Array<string> | Promise<ISuggestionsResponse | void>, string>(function*(text: string) {
    if (text.length > 2) {
      const cachedKeywords = keywordResultsCache[String(text)];
      if (!isEmpty(cachedKeywords)) {
        return cachedKeywords;
      }

      // debounce: see https://ember-power-select.com/cookbook/debounce-searches/
      yield timeout(200);

      const response: ISuggestionsResponse = yield readSuggestions({ input: text });
      keywordResultsCache[String(text)] = response.source;
      return response.source;
    } else {
      return [];
    }
  }).restartable();

  /**
   * When search actually happens, then we transition to a new route.
   * @param text search term
   */
  onSearch(text: string): void {
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
