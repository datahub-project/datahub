import Component from '@ember/component';
import { readSuggestions, ISuggestionsResponse } from 'wherehows-web/utils/api/search/suggestions';
import { RouterService } from 'ember';
import { service } from '@ember-decorators/service';
import { task, timeout } from 'ember-concurrency';
import { computed } from '@ember-decorators/object';
import SearchService from 'wherehows-web/services/search';

export default class SearchBoxContainer extends Component {
  @service
  router: RouterService;

  @service
  search: SearchService;

  placeholder: string = 'Search datasets by keywords... e.g. pagekey';

  suggestions: Array<string>;

  @computed('search.keyword')
  get keyword(): string {
    return this.search.keyword;
  }

  onUserType = task(function*(this: SearchBoxContainer, text: string) {
    if (text.length > 2) {
      yield timeout(500);
      const response: ISuggestionsResponse = yield readSuggestions({ input: text });
      return response.source;
    }
  }).restartable();

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
