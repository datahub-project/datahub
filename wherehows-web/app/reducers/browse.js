import { ActionTypes } from 'wherehows-web/actions/browse';

/**
 * Initial state for browse feature
 * @type {{entity: string}}
 */
const initialState = {
  entity: 'datasets',
  isFetching: false,
  browseData: {}
};

/**
 * Reduces browse actions into state object
 * @param {Object} state = initialState
 * @param {Object} action Flux Standard Action FSA object
 * @return {Object} final state of the browse key in the store
 */
export default (state = initialState, action = {}) => {
  switch (action.type) {
    case ActionTypes.REQUEST_BROWSE_DATA:
    case ActionTypes.SELECT_BROWSE_DATA:
      return Object.assign({}, state, {
        isFetching: true,
        entity: action.payload.entity
      });

    case ActionTypes.RECEIVE_BROWSE_DATA:
      return Object.assign({}, state, {
        isFetching: false,
        browseData: action.payload.browseData
      });

    default:
      return state;
  }
};
