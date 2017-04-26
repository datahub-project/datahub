import fetch from 'ember-network/fetch';
import buildUrl from 'wherehows-web/utils/build-url';

/**
 *
 * @param {String} entity
 */
const fetchPagedEntities = entity => getState => {
  // Extract the props `baseURL` & `page` from the redux store for the given `entity`
  const { [entity]: { baseURL, query: { page } } } = getState();
  const pageURL = buildUrl(baseURL, 'page', page);

  return fetch(pageURL).then(response => response.json());
};

/**
 * fetches the child entities for a entity with specified by urn and or page
 * @param {String} entity
 */
const fetchPagedUrnEntities = entity => getState => {
  const { [entity]: { baseURL }, browseEntity: { [entity]: { query = {} } = {} } = {} } = getState();

  const pagedUrnURL = Object.keys(query).reduce((url, queryKey) => {
    let queryValue = query[queryKey];
    if (queryValue) {
      if (queryKey === 'urn' && queryValue.slice(-1) !== '/') {
        queryValue = `${queryValue}/`;
      }

      return buildUrl(url, queryKey, queryValue);
    }
    return url;
  }, baseURL);

  return fetch(pagedUrnURL).then(response => response.json());
};

export { fetchPagedEntities, fetchPagedUrnEntities };
