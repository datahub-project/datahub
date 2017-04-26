import { ActionTypes } from 'wherehows-web/actions/browse/entity';

/**
 * Initial state for browse.entity feature
 * @type {{entity: string}}
 */
const initialState = {
  currentEntity: 'datasets',
  isFetching: false,
  datasets: { listURL: '', query: {} },
  flows: { listURL: '', query: {} },
  metrics: { listURL: '', query: {} }
};

/**
 * Reduces browse.entity actions into state object
 * @param {Object} state = initialState
 * @param {Object} action Flux Standard Action FSA object
 * @return {Object}
 */
export default (state = initialState, action = {}) => {
  const { payload = {} } = action;
  const currentEntity = payload.entity;
  const previousProps = state[currentEntity];

  switch (action.type) {
    case ActionTypes.REQUEST_NODE_LIST:
      return Object.assign({}, state, {
        currentEntity,
        isFetching: true,
        [currentEntity]: Object.assign({}, previousProps, {
          listURL: payload.listURL,
          query: payload.query,
          queryParams: payload.queryParams
        })
      });

    case ActionTypes.RECEIVE_NODE_LIST:
      return Object.assign({}, state, {
        isFetching: false
      });

    default:
      return state;
  }
};
