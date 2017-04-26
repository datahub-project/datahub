import Ember from 'ember';
import fetch from 'ember-network/fetch';
import buildUrl from 'wherehows-web/utils/build-url';
const { isBlank } = Ember;

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

/**
 *
 * @param entity
 */
const fetchNodes = entity => getState => {
  const { browseEntity: { [entity]: { listURL = '', query = {}, queryParams = [] } = {} } = {} } = getState();

  /**
   * Constructs the list url based on the current browseEntity
   * @type {String}
   */
  const nodeURL = queryParams.reduce((url, queryParam) => {
    let queryValue = query[queryParam];

    if (!isBlank(queryValue)) {
      // Ensure that the last char in a URN in a /.
      //   /api/vi/list/ Api behaviour is sensitive to the presence of trailing slash
      //   and will result differently
      if (queryParam === 'urn' && queryValue.slice(-1) !== '/') {
        queryValue = `${queryValue}/`;
      }

      return buildUrl(url, queryParam, queryValue);
    }

    return url;
  }, `${listURL}/${entity}`);

  // TODO: DSS-7019 remove any parsing from response objects. in createLazyRequest and update all call sites
  return fetch(nodeURL).then(response => response.json()).then(({ status, nodes: data }) => ({ status, data }));
};

export { fetchPagedEntities, fetchPagedUrnEntities, fetchNodes };
