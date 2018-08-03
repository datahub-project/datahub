/**
 * Takes a requestActionCreator to trigger the action indicating that a request
 *   has occurred in the application, and a receiverActionCreator to trigger, indicating
 *   that data for the request has been received.
 * @param {Function} requesterActionCreator redux action creator
 * @param {Function} receiverActionCreator redux action creator
 * @param asyncExecutor
 * @return {function(String, Number|String): Function}
 */
const createLazyRequest = (
  requesterActionCreator,
  receiverActionCreator,
  asyncExecutor
  /**
   *
   * @param {Object} props = {}
   */
) => (props = {}) =>
  /**
   * Redux Thunk. Function takes a store dispatch, and unused getState that get passed
   *   into the returned function from createAsyncThunk
   * @arg {Function} dispatch callback passed in by redux-thunk
   */
  function(dispatch) {
    dispatch(requesterActionCreator(Object.assign({}, props)));
    return createAsyncThunk(receiverActionCreator, asyncExecutor)(...arguments);
  };

/**
 * Async action creator, fetches a list of entities for the currentPage in the
 *   store
 * @param {Function} receiverActionCreator
 * @param {Function} asyncExecutor
 */
const createAsyncThunk = (
  receiverActionCreator,
  asyncExecutor
  /**
   *
   * @param {Function} dispatch callback for action dispatch from redux thunk
   * @param {Function} getState callback to get the current store state
   * @return {Promise.<*>}
   */
) => async (dispatch, getState) => {
  const response = await asyncExecutor(getState);

  if (response.status === 'ok') {
    return dispatch(receiverActionCreator(response));
  }

  return dispatch(receiverActionCreator(new Error(`Request failed with status ${status}`)));
};

export { createLazyRequest };
