import { initializeState, createUrnMapping, receiveEntities, createPageMapping } from 'wherehows-web/reducers/entities';
import { ActionTypes } from 'wherehows-web/actions/flows';

/**
 * Takes the `flows` slice of the state tree and performs the specified reductions for each action
 * @param {Object} state = initialState the slice of the state object representing flows
 * @param {Object} action Flux Standard Action representing the action to be preformed on the state
 * @prop {String} action.type actionType
 * @return {Object}
 */
export default (state = initializeState(), action = {}) => {
  switch (action.type) {
    case ActionTypes.RECEIVE_PAGED_FLOWS:
      return Object.assign({}, state, {
        isFetching: false,
        byUrn: createUrnMapping('flows')(state.byUrn, action.payload),
        byId: receiveEntities('flows')(state.byId, action.payload),
        byPage: createPageMapping('flows')(state.byPage, action.payload)
      });

    case ActionTypes.SELECT_PAGED_FLOWS:
    case ActionTypes.REQUEST_PAGED_FLOWS:
      return Object.assign({}, state, {
        query: Object.assign({}, state.query, {
          page: action.payload.page
        }),
        baseURL: action.payload.baseURL,
        isFetching: true
      });

    default:
      return state;
  }
};
