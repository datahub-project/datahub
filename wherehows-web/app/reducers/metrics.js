import { initialState, receiveEntities } from 'wherehows-web/reducers/entities';
import { ActionTypes } from 'wherehows-web/actions/metrics';

/**
 * Reduces the store state for metrics
 * @param {Object} state = initialState the slice of the state object representing metrics
 * @param {Object} action Flux Standard Action representing the action to be preformed on the state
 * @prop {String} action.type actionType
 * @return {Object}
 */
export default (state = initialState, action = {}) => {
  switch (action.type) {
    case ActionTypes.RECEIVE_PAGED_METRICS:
      return receiveEntities('metrics')(state, action.payload);

    case ActionTypes.SELECT_PAGED_METRICS:
    case ActionTypes.REQUEST_PAGED_METRICS:
      return Object.assign({}, state, {
        currentPage: action.payload.page,
        pageBaseURL: action.payload.pageBaseURL,
        isFetching: true
      });

    default:
      return state;
  }
};
