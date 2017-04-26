import {
  initializeState,
  receiveNodes,
  receiveEntities,
  createUrnMapping,
  createPageMapping
} from 'wherehows-web/reducers/entities';
import { ActionTypes as DatasetActions } from 'wherehows-web/actions/datasets';
import { ActionTypes } from 'wherehows-web/actions/browse/entity';

/**
 * datasets root reducer
 * Takes the `datasets` slice of the state tree and performs the specified reductions for each action
 * @param {Object} state slice of the state tree this reducer is responsible for
 * @param {Object} action Flux Standard Action representing the action to be preformed on the state
 * @prop {String} action.type actionType
 * @return {Object}
 */
export default (state = initializeState(), action = {}) => {
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
        isFetching: false,
        byId: receiveEntities('datasets')(state.byId, action.payload),
        byUrn: createUrnMapping('datasets')(state.byUrn, action.payload),
        byPage: createPageMapping('datasets')(state.byPage, action.payload)
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
