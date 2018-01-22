import { isBlank } from '@ember/utils';
import fetch from 'fetch';
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

  return fetch(pagedUrnURL)
    .then(response => response.json())
    .then((payload = {}) => {
      // retain the urn that was initiated with this request on the result data object
      if (payload.status === 'ok') {
        payload.data = Object.assign({}, payload.data, {
          parentUrn: query.urn || null
        });
      }

      return payload;
    });
};

/**
 * Takes a entity and returns a function to fetch entities by an entity url with a `name` segment
 * @param {String} entity
 */
const fetchNamedPagedEntities = entity => getState => {
  const { [entity]: { baseURL }, browseEntity: { [entity]: { query } } = {} } = getState();
  const queryCopy = Object.assign({}, query);
  const name = queryCopy.name;
  let baseNameUrl = baseURL;

  if (name) {
    baseNameUrl = `${baseNameUrl}/name/${name}`;
    delete queryCopy.name;
  }

  const namedPageURL = Object.keys(queryCopy).reduce((url, queryKey) => {
    let queryValue = queryCopy[queryKey];
    if (queryValue) {
      return buildUrl(url, queryKey, queryValue);
    }

    return url;
  }, baseNameUrl);

  return fetch(namedPageURL)
    .then(response => response.json())
    .then((payload = {}) => {
      if (payload.status === 'ok') {
        payload.data = Object.assign({}, payload.data, {
          parentName: name || null
        });
      }

      return payload;
    });
};

/**
 * Takes a entity name and returns a function the fetches an entity by urn path segment
 * @param {String} entity
 */
const fetchUrnPathEntities = entity => getState => {
  const { [entity]: { baseURL }, browseEntity: { [entity]: { query } = {} } = {} } = getState();
  const queryCopy = Object.assign({}, query);
  const urn = queryCopy.urn;
  let baseUrnUrl = baseURL;

  // If the urn exists, append its value to the base url for the entity and remove the attribute from the local
  //   copy of the queried parameters
  if (urn) {
    baseUrnUrl = `${baseUrnUrl}/${urn}`;
    delete queryCopy.urn;
  }

  // Append the left over query params to the constructed url string
  const urnPathUrl = Object.keys(query).reduce((url, queryKey) => {
    let queryValue = query[queryKey];
    if (queryValue) {
      return buildUrl(url, queryKey, queryValue);
    }

    return url;
  }, baseUrnUrl);

  return fetch(urnPathUrl)
    .then(response => response.json())
    .then((payload = {}) => {
      if (payload.status === 'ok') {
        payload.data = Object.assign({}, payload.data, {
          parentUrn: urn || null
        });
      }

      return payload;
    });
};

/**
 * Request urn child nodes/ datasets for the specified entity
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
  return fetch(nodeURL)
    .then(response => response.json())
    .then((payload = {}) => {
      return Object.assign({}, payload, {
        parentUrn: query.urn || null
      });
    });
};

/**
 * For a given entity name, fetches the nodes at the list url by appending the entity name and name path as segments
 *   of the request url
 * @param {String} entity
 */
const fetchNamedEntityNodes = entity => getState => {
  // TODO: DSS-7019 rename queryParams to queryList, don't over load the name `queryParams`
  const { browseEntity: { [entity]: { listURL = '', query: { name } } = {} } = {} } = getState();
  const namePath = name ? `/${name}` : '';
  const nodeURL = `${listURL}/${entity}${namePath}`;

  return fetch(nodeURL)
    .then(response => response.json())
    .then((payload = {}) => {
      return Object.assign({}, payload, {
        //TODO: Should this be namedEntityNodes vs urnPathEntityNodes
        parentName: name || null
      });
    });
};

/**
 * For a given entity, fetches the entities nodes at the list url by appending the entity name and the urn path
 *   as segments of the request url
 * @param {String} entity
 */
const fetchUrnPathEntityNodes = entity => getState => {
  const { browseEntity: { [entity]: { listURL = '', query: { urn } } = {} } = {} } = getState();
  const urnPath = urn ? `/${urn}` : '';
  const urnListUrl = `${listURL}/${entity}${urnPath}`;

  return fetch(urnListUrl)
    .then(response => response.json())
    .then((payload = {}) => {
      return Object.assign({}, payload, {
        //TODO: Should this be namedEntityNodes vs urnPathEntityNodes
        parentUrn: urn || null
      });
    });
};

export {
  fetchPagedEntities,
  fetchPagedUrnEntities,
  fetchNodes,
  fetchNamedPagedEntities,
  fetchNamedEntityNodes,
  fetchUrnPathEntities,
  fetchUrnPathEntityNodes
};
