import Component from '@ember/component';
import { classNames } from '@ember-decorators/component';
import { inject as service } from '@ember/service';
import Search from 'wherehows-web/services/search';
import { action } from '@ember/object';

/**
 * Defines the SearchResult class which renders an individual search result item and performs / invokes some
 * analytics related tasks to track content impression and interaction
 * @export
 * @class SearchResult
 * @extends {Component}
 */
@classNames('search-result')
export default class SearchResult extends Component {
  /**
   * Reference to the search service
   * @type {Search}
   * @memberof SearchResult
   */
  @service
  search: Search;

  /**
   * When a search result is clicked with the intent of navigating to it inform
   * the search service.
   * This allows us to explicity indicate that a transition to a search result is in progress
   * @param {string} resultUrn urn for the search result item
   * @param {MouseEvent} _e
   * @memberof SearchResult
   */
  @action
  onResultClick(resultUrn: string, _e: MouseEvent) {
    this.search.didClickSearchResult(resultUrn);
  }
}
