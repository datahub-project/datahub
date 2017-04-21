import { initialState, receiveEntities } from 'wherehows-web/reducers/entities';
import { ActionTypes } from 'wherehows-web/actions/flows';

/**
 * Reduces the store state for flows
 * @param {Object} state = initialState the slice of the state object representing flows
 * @param {Object} action Flux Standard Action representing the action to be preformed on the state
 * @prop {String} action.type actionType
 * @return {Object}
 */
export default (state = initialState, action = {}) => {
  switch (action.type) {
    case ActionTypes.RECEIVE_PAGED_FLOWS:
      return receiveEntities('flows')(state, action.payload);

    case ActionTypes.SELECT_PAGED_FLOWS:
    case ActionTypes.REQUEST_PAGED_FLOWS:
      return Object.assign({}, state, {
        currentPage: action.payload.page,
        pageBaseURL: action.payload.pageBaseURL,
        isFetching: true
      });

    default:
      return state;
  }
};
