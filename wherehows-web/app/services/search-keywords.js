import Ember from 'ember';
import buildUrl from 'wherehows-web/utils/build-url';
import { isEmpty } from '@ember/utils';
import Service from '@ember/service';

const { $: { getJSON } } = Ember;

/**
 * Runtime cache of recently seen typeahead results
 * @type {Object.<Object>} a hash of urls to results
 */
const keywordResultsCache = {};

/**
 * a reference to to most recent typeahead query
 * @type {String}
 */
let lastSeenQuery = '';

/**
 * Map of routeNames to routes
 * @param {String} routeName name of the route to return
 * @return {String} route url of the keyword route
 */
const keywordRoutes = routeName =>
  ({
    datasets: '/api/v1/autocomplete/datasets',
    metrics: '/api/v1/autocomplete/metrics',
    flows: '/api/v1/autocomplete/flows'
  }[routeName] || '/api/v1/autocomplete/search');

/**
 * Retrieves the keywords for a given url
 * @param {String} url the url path to the fetch typeahead keywords from
 * @return {Promise.<Object|void>}
 */
const getKeywordsFor = url =>
  new Promise(resolve => {
    // If we've seen the url for this request in the given session
    //   there is no need to make a new request. For example when
    //   a user is backspacing in the search bar
    const cachedKeywords = keywordResultsCache[String(url)];
    if (!isEmpty(cachedKeywords)) {
      return resolve(cachedKeywords);
    }

    getJSON(url).then(response => {
      const { status } = response;
      status === 'ok' && resolve((keywordResultsCache[String(url)] = response));
    });
  });

/**
 * Curried function that takes a source to filter on and then the queryResolver
 *   function
 * @param {('datasets'|'metrics'|'flows')} source filters the keywords results
 *   on the provided source
 * @inner {function([String, Function, Function])} a queryResolver
 *   function
 */
const apiResultsFor = (source = 'datasets') => ([query, , asyncResults]) => {
  const autoCompleteBaseUrl = keywordRoutes(source);
  const url = buildUrl(autoCompleteBaseUrl, 'input', query);
  lastSeenQuery = query;

  getKeywordsFor(url)
    .then(({ source = [], input }) => {
      if (input === lastSeenQuery) {
        return source;
      }
    })
    .then(asyncResults);
};

export default Service.extend({
  apiResultsFor
});
