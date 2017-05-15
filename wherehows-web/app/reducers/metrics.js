import {
  initializeState,
  receiveEntities,
  namesToNodeName,
  createPageMapping,
  createNameMapping
} from 'wherehows-web/reducers/entities';
import { ActionTypes } from 'wherehows-web/actions/metrics';

/**
 * Sets the default initial state for metrics slice. Appends a byName property to the shared representation.
 * @type {Object}
 */
const initialState = Object.assign({}, initializeState(), {
  byName: {},
  nodesByName: []
});

/**
 * Takes the `metrics` slice of the state tree and performs the specified reductions for each action
 * @param {Object} state = initialState slice of the state tree this reducer is responsible for
 * @param {Object} action Flux Standard Action representing the action to be preformed on the state
 * @prop {String} action.type actionType
 * @return {Object}
 */
export default (state = initialState, action = {}) => {
  switch (action.type) {
    // Action indicating a request for metrics by page
    case ActionTypes.SELECT_PAGED_METRICS:
    case ActionTypes.REQUEST_PAGED_METRICS:
      return Object.assign({}, state, {
        query: Object.assign({}, state.query, action.payload.query),
        baseURL: action.payload.baseURL || state.baseURL,
        isFetching: true
      });

    // Action indicating a receipt of metrics by page
    case ActionTypes.REQUEST_PAGED_NAMED_METRICS:
      return Object.assign({}, state, {
        query: Object.assign({}, state.query, action.payload.query)
      });

    case ActionTypes.RECEIVE_PAGED_METRICS:
    case ActionTypes.RECEIVE_PAGED_NAMED_METRICS:
      return Object.assign({}, state, {
        isFetching: false,
        byId: receiveEntities('metrics')(state.byId, action.payload),
        byPage: createPageMapping('metrics')(state.byPage, action.payload),
        byName: createNameMapping('metrics')(state.byName, action.payload)
      });

    case ActionTypes.REQUEST_METRICS_NODES:
      return Object.assign({}, state, {
        query: Object.assign({}, state.query, action.payload.query),
        isFetching: true
      });

    case ActionTypes.RECEIVE_METRICS_NODES: // Action indicating a receipt of list nodes / datasets for dataset urn
      return Object.assign({}, state, {
        isFetching: false,
        nodesByName: namesToNodeName(state, action.payload)
      });

    default:
      return state;
  }
};
