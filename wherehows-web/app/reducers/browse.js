import { ActionTypes } from 'wherehows-web/actions/browse';

/**
 * Initial state for browse feature
 * @type {{viewingEntity: string}}
 */
const initialState = {
  viewingEntity: 'datasets'
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
        viewingEntity: action.payload.viewingEntity
      });

    case ActionTypes.RECEIVE_BROWSE_DATA:
      return Object.assign({}, state, {
        isFetching: false
      });

    default:
      return state;
  }
};
