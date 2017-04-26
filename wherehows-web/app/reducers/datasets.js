import { initializeState, combinedReducer, receiveNodes } from 'wherehows-web/reducers/entities';
import { ActionTypes as DatasetActions } from 'wherehows-web/actions/datasets';
import { ActionTypes } from 'wherehows-web/actions/browse/entity';

/**
 * Takes the `datasets` slice of the state tree and performs the specified reductions for each action
 * @param {Object} state slice of the state tree this reducer is responsible for
 * @param {Object} action Flux Standard Action representing the action to be preformed on the state
 * @prop {String} action.type actionType
 * @return {Object}
 */
const aggregateReducer = (state, action = {}) => {
  switch (action.type) {
    // Action indicating a request for datasets by page
    case DatasetActions.SELECT_PAGED_DATASETS:
    case DatasetActions.REQUEST_PAGED_DATASETS:
      return Object.assign({}, state, {
        query: Object.assign({}, state.query, {
          page: action.payload.page
        }),
        baseURL: action.payload.baseURL,
        isFetching: true
      });
    // Action indicating a receipt of datasets by page
    case DatasetActions.RECEIVE_PAGED_DATASETS:
    case DatasetActions.RECEIVE_PAGED_URN_DATASETS:
      return Object.assign({}, state, {
        isFetching: false
      });

    case ActionTypes.SELECT_PAGED_DATASETS:
    case ActionTypes.REQUEST_PAGED_DATASETS:
      return Object.assign({}, state, {
        currentPage: action.payload.page,
        pageBaseURL: action.payload.pageBaseURL,
        isFetching: true
      });

    default:
      return state;
  }
};

/**
 * datasets root reducer
 * @param {Object} state slice of the top-level state tree for datasets
 * @param {Object} action Flux Standard Action representing the action to be preformed on the state
 * @return {Object}
 */
export default (
  state = initializeState(),
  action
) => {
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
