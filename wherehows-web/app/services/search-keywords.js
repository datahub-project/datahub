import Ember from 'ember';

const {
  Service,
  isEmpty,
  $: { getJSON }
} = Ember;

const keywordRoutes = routeName => {
  // These routes are temporary until the platform routes are available
  const route = {
    dataset: '/api/v1/autocomplete/search',
    metric: '/api/v1/autocomplete/search',
    flow: '/api/v1/autocomplete/search'
  }[routeName];

  return route || '/api/v1/autocomplete/search';
};

const getKeywordsFor = ({ source = 'all' } = {}) =>
  new Promise(resolve => {
    const cachedKeywords = keywordCache[source];
    if (!isEmpty(cachedKeywords)) {
      return resolve(cachedKeywords);
    }

    Promise.resolve(getJSON(keywordRoutes(source)))
      .then(({ source = [] }) => source)
      .then(keywords => keywordCache[source] = keywords)
      .then(resolve);
  });

const keywordCache = {
  dataset: [],
  metric: [],
  flow: [],
  all: []
};

const queryResolver = (query, syncResults, asyncResults) => {
  const regex = new RegExp(`.*${query}.*`, 'i');

  getKeywordsFor()
    .then(keywords => keywords.filter(keyword => regex.test(keyword)))
    .then(asyncResults);
};

export default Service.extend({
  queryResolver,
  keywordsFor: ({ source = 'all' } = {}) => getKeywordsFor(source)
});
