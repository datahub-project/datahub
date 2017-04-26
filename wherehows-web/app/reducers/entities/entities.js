import _ from 'lodash';
import { mapEntitiesToIds, mapUrnsToIds } from 'wherehows-web/reducers/utils';

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
  entityName /**
   *
   * @param {Object} prevEntities current list of ids mapped to entities
   * @param {Object} props
   * @props {Array} props[entityName] list of received entities
   */
) => (prevEntities, props) => merge({}, prevEntities, mapEntitiesToIds(props[entityName]));

/**
 * Appends a unique urn as key with a corresponding id as value
 * @param {String} entityName
 */
const appendUrnIdMap = (
  entityName
  /**
   * @param {Object} previousEntities current mapping of Urns to id
   * @param {Object} props payload with new objects containing id and urn
   */
) => (previousEntities, props) => merge({}, previousEntities, mapUrnsToIds(props[entityName]));

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
 * @return {Object}
 */
const urnsToNodeUrn = (state, { data: nodes = [] }) => {
  const { query: { urn }, nodesByUrn } = state;
  return Object.assign({}, nodesByUrn, {
    [urn]: union(nodes)
  });
};

/**
 * Takes an identifier for an entity and returns a reducing function with the entityName as context
 * @param {String} entityName
 */
const receiveEntities = (
  entityName /**
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
 *
 * @param {String} entityName
 */
const createPageMapping = entityName => (state, payload = {}) => {
  return entitiesToPage(entityName)(state, payload);
};

/**
 * Takes the response from the list api request and invokes a function to
 *   map a urn to child urns or nodes
 * @param {Object} state
 * @param {Object} payload the response from the list endpoint/api
 * @return {Object}
 */
const receiveNodes = (state, payload = {}) => urnsToNodeUrn(state, payload);

export { receiveNodes, initializeState, receiveEntities, createUrnMapping, createPageMapping };
