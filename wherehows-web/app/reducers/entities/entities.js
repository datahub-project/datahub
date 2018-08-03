import _ from 'lodash';
import { mapEntitiesToIds } from 'wherehows-web/reducers/utils';

const { merge, union } = _;

// Initial state for entities (metrics, flows, datasets)
const _initialState = {
  count: null,
  page: null,
  itemsPerPage: null,
  totalPages: null,
  query: {
    urn: '',
    page: ''
  },
  byId: {},
  byPage: {},
  byUrn: {}
};

/**
 * Ensure we deep clone since this shape is shared amongst entities
 * @return {Object}
 */
const initializeState = () => JSON.parse(JSON.stringify(_initialState));

/**
 * Merges previous entities and a new map of ids to entities into a new map
 * @param {String} entityName
 */
const appendEntityIdMap = (
  entityName
  /**
   *
   * @param {Object} prevEntities current list of ids mapped to entities
   * @param {Object} props
   * @props {Array} props[entityName] list of received entities
   */
) => (prevEntities, props) => merge({}, prevEntities, mapEntitiesToIds(props[entityName]));

/**
 * Appends a list of child entity ids for a given urn. urn is null for top level entities
 * @param {String} entityName
 */
const appendUrnIdMap = (
  entityName
  /**
   * @param {Object} urnEntities current mapping of Urns to ids
   * @param {Object} props payload with new objects containing id, and urn prop
   */
) => (urnEntities, { parentUrn = null, [entityName]: entities = [] }) =>
  Object.assign({}, urnEntities, {
    [parentUrn]: union(urnEntities[parentUrn], entities.mapBy('id'))
  });

/**
 * Appends a list of child entities ids for a given name. name is null for top level entities
 * @param {String} entityName
 */
const appendNamedIdMap = entityName => (nameEntities, { parentName = null, [entityName]: entities = [] }) =>
  Object.assign({}, nameEntities, {
    [parentName]: union(nameEntities[parentName], entities.mapBy('id'))
  });

/**
 * Returns a curried function that receives entityName to lookup on the props object
 * @param {String} entityName
 * @return {Function}
 */
const entitiesToPage = (
  entityName
  /**
   *  a new map of page numbers to datasetIds
   * @param {Object} pagedEntities
   * @param {Object} props
   * @props {Array} props[entityName] list of received entities
   * @return {Object} props.page page the page that contains the returned list of entities
   */
) => (pagedEntities = {}, props) => {
  const entities = props[entityName];
  const { page } = props;

  return Object.assign({}, pagedEntities, {
    [page]: union(pagedEntities[page], entities.mapBy('id'))
  });
};

/**
 * Maps a urn to a list of child nodes from the list api
 * @param {Object} state
 * @param {Array} nodes
 * @param {String} parentUrn
 * @return {*}
 */
const urnsToNodeUrn = (state, { nodes = [], parentUrn = null } = {}) => {
  const { nodesByUrn } = state;
  return Object.assign({}, nodesByUrn, {
    [parentUrn]: union(nodes)
  });
};

/**
 * Maps a name to a list of child nodes from the list api
 * @param {Object} state
 * @param {Array} nodes
 * @param {String} parentName
 * @return {*}
 */
const namesToNodeName = (state, { nodes = [], parentName = null } = {}) => {
  const { nodesByName } = state;
  return Object.assign({}, nodesByName, {
    [parentName]: union(nodes)
  });
};

/**
 * Takes an identifier for an entity and returns a reducing function with the entityName as context
 * @param {String} entityName
 */
const receiveEntities = (
  entityName
  /**
   * entities (flows|metrics|datasets) for the ActionTypes.RECEIVE_PAGED_[ENTITY_NAME] action
   * @param {Object} state previous state for datasets
   * @param {Object} payload data received through ActionTypes.RECEIVE_PAGED_[ENTITY_NAME]
   * @return {Object}
   */
) => (state, payload = {}) => {
  return appendEntityIdMap(entityName)(state, payload);
};

/**
 *
 * @param {String} entityName
 */
const createUrnMapping = entityName => (state, payload = {}) => {
  return appendUrnIdMap(entityName)(state, payload);
};

/**
 * Curries an entityName into a function to append named entity entities
 * @param {String} entityName
 */
const createNameMapping = entityName => (state, payload = {}) => {
  return appendNamedIdMap(entityName)(state, payload);
};

/**
 * Appends a list of entities to a page
 * @param {String} entityName
 */
const createPageMapping = entityName => (state, payload = {}) => {
  return entitiesToPage(entityName)(state, payload);
};

export {
  initializeState,
  namesToNodeName,
  urnsToNodeUrn,
  receiveEntities,
  createUrnMapping,
  createPageMapping,
  createNameMapping
};
