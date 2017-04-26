import { ActionTypes } from 'wherehows-web/actions/browse/entity';

/**
 * Initial state for browse.entity feature
 * @type {{currentEntity: string, isFetching: boolean, datasets: {listURL: string, query: {}, queryParams: Array}, flows: {listURL: string, query: {}, queryParams: Array}, metrics: {listURL: string, query: {}, queryParams: Array}}}
 */
const initialState = {
  currentEntity: 'datasets',
  isFetching: false,
  datasets: { listURL: '', query: {}, queryParams: [] },
  flows: { listURL: '', query: {}, queryParams: [] },
  metrics: { listURL: '', query: {}, queryParams: [] }
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
