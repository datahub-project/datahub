import {
  initializeState,
  createUrnMapping,
  receiveEntities,
  createPageMapping,
  urnsToNodeUrn
} from 'wherehows-web/reducers/entities';
import { ActionTypes } from 'wherehows-web/actions/flows';

/**
 * Sets the default initial state for flows slice. Appends a byName property to the shared representation.
 * @type {Object}
 */
const initialState = Object.assign({}, initializeState(), {
  nodesByUrn: []
});
/**
 * Takes the `flows` slice of the state tree and performs the specified reductions for each action
 * @param {Object} state = initialState the slice of the state object representing flows
 * @param {Object} action Flux Standard Action representing the action to be preformed on the state
 * @prop {String} action.type actionType
 * @return {Object}
 */
export default (state = initialState, action = {}) => {
  switch (action.type) {
    case ActionTypes.RECEIVE_PAGED_FLOWS:
    case ActionTypes.RECEIVE_PAGED_URN_FLOWS:
      return Object.assign({}, state, {
        isFetching: false,
        byId: receiveEntities('flows')(state.byId, action.payload),
        byPage: createPageMapping('flows')(state.byPage, action.payload),
        byUrn: createUrnMapping('flows')(state.byUrn, action.payload)
      });

    case ActionTypes.REQUEST_FLOWS_NODES:
      return Object.assign({}, state, {
        query: Object.assign({}, state.query, action.payload.query),
        isFetching: true
      });

    case ActionTypes.RECEIVE_FLOWS_NODES:
      return Object.assign({}, state, {
        isFetching: false,
        nodesByUrn: urnsToNodeUrn(state, action.payload)
      });

    case ActionTypes.REQUEST_PAGED_URN_FLOWS:
      return Object.assign({}, state, {
        query: Object.assign({}, state.query, action.payload.query)
      });

    case ActionTypes.SELECT_PAGED_FLOWS:
    case ActionTypes.REQUEST_PAGED_FLOWS:
      return Object.assign({}, state, {
        query: Object.assign({}, state.query, {
          page: action.payload.query.page
        }),
        baseURL: action.payload.baseURL || state.baseURL,
        isFetching: true
      });

    default:
      return state;
  }
};
