import { mapEntitiesToIds } from 'wherehows-web/reducers/utils';
import _ from 'lodash';

const { merge, union } = _;

/**
 * Initial entity (datasets|flows|metrics) slice
 * @type {{count: null, page: null, itemsPerPage: null, totalPages: null, currentPage: null, isFetching: boolean, pageBaseURL: string, byId: {}, byPage: {}}}
 */
const initialState = {
  count: null,
  page: null,
  itemsPerPage: null,
  totalPages: null,
  currentPage: null,
  isFetching: false,
  pageBaseURL: '',

  byId: {},

  byPage: {}
};

/**
 * Merges previous entities and a new map of ids to entities into a new map
 * @param entityName
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
 * Returns a curried function that receives entityName to lookup on the props object
 * @param {String} entityName
 * @return {Function}
 */
const entitiesToPage = (
  entityName /**
   * Returns a new map of page numbers to datasetIds
   * @param {Object} pagedEntities
   * @param {Object} props
   * @props {Array} props[entityName] list of received entities
   * @return {Object} props.page page the page that contains the returned list of entities
   */
) => (pagedEntities = {}, props) => {
  const entities = props[entityName];
  const { page } = props;
  return Object.assign({}, pagedEntities, {
    [page]: union(pagedEntities[page] || [], entities.mapBy('id'))
  });
};

/**
 * Takes an identifier for an entity and returns a reducing function with the entityName as context
 * @param {String} entityName
 */
const receiveEntities = (
  entityName /**
   * Composes entities (flows|metrics|datasets) for the ActionTypes.RECEIVE_PAGED_[ENTITY_NAME] action
   * @param {Object} state previous state for datasets
   * @param {Object} payload data received through ActionTypes.RECEIVE_PAGED_[ENTITY_NAME]
   * @return {Object}
   */
) => (state, payload = {}) => {
  const { count, page, itemsPerPage, totalPages } = payload;

  return Object.assign({}, state, {
    page,
    count,
    totalPages,
    itemsPerPage,
    isFetching: false,
    byId: appendEntityIdMap(entityName)(state.byId, payload),
    byPage: entitiesToPage(entityName)(state.byPage, payload)
  });
};

export { initialState, receiveEntities };
