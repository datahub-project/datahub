import { initializeState, combinedReducer } from 'wherehows-web/reducers/entities';
import { ActionTypes } from 'wherehows-web/actions/metrics';

/**
 * Takes the `datasets` slice of the state tree and performs the specified reductions for each action
 * @param {Object} state slice of the state tree this reducer is responsible for
 * @param {Object} action Flux Standard Action representing the action to be preformed on the state
 * @prop {String} action.type actionType
 * @return {Object}
 */
const aggregateReducer = (state, action = {}) => {
  switch (action.type) {
    // Action indicating a request for metrics by page
    case ActionTypes.SELECT_PAGED_METRICS:
    case ActionTypes.REQUEST_PAGED_METRICS:
      return Object.assign({}, state, {
        query: Object.assign({}, state.query, {
          page: action.payload.page
        }),
        baseURL: action.payload.baseURL,
        isFetching: true
      });
    // Action indicating a receipt of metrics by page
    case ActionTypes.RECEIVE_PAGED_METRICS:
      return Object.assign({}, state, {
        isFetching: false
      });

    default:
      return state;
  }
};

/**
 * Reduces the store state for metrics
 * @param {Object} state = initialState the slice of the state object representing metrics
 * @param {Object} action Flux Standard Action representing the action to be preformed on the state
 * @prop {String} action.type actionType
 * @return {Object}
 */
export default (state = initializeState(), action = {}) => {
  // Extract the following props from the root state
  //  then create a segregated object to pass to the combinedReducer. Redux.combineReducer expects a the pre-loaded
  //  state to only contain properties that will be assigned to the each slice it is responsible for and nothing more.
  const { byId, byPage, byUrn } = state;
  const slice = Object.assign(
    {},
    {
      byId,
      byPage,
      byUrn
    }
  );

  // Merge the combined props from slice reducer functions and the current state object to an intermediate state
  //  this intermediate state is consumed by the aggregate reducer to generate the final state for this reducer level
  const intermediateState = Object.assign({}, state, combinedReducer(slice, action));
  return aggregateReducer(intermediateState, action);
};
