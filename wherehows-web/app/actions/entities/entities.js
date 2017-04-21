import fetch from 'ember-network/fetch';

/**
 * Takes a requestActionCreator to trigger the action indicating that an entity request
 *   has occurred in the application, and a receiverActionCreator to trigger, indicating
 *   that data for the request has been received.
 * @param {String} entity the entity context when creating lazyRequest
 * @param {Function} requesterActionCreator redux action creator
 * @param {Function} receiverActionCreator redux action creator
 * @return {function(String, Number|String): Function}
 */
const createLazyRequest = (
  entity,
  requesterActionCreator,
  receiverActionCreator /**
   *
   * @param {String} pageBaseURL url for the request
   * @param {Number|String} page
   */
) => (pageBaseURL, page) =>
  /**
   * Redux Thunk. Function takes a store dispatch, and unused getState that get passed
   *   into the returned function from createAsyncThunk
   * @arg {Function} dispatch callback passed in by redux-thunk
   */
  function(dispatch) {
    dispatch(requesterActionCreator({ pageBaseURL, page }));
    return createAsyncThunk(entity, receiverActionCreator)(...arguments);
  };

/**
 * Async action creator, fetches a list of entities for the currentPage in the
 *   store
 * @param {String} entity to extract props from in the redux store
 * @param {Function} receiverActionCreator
 */
const createAsyncThunk = (
  entity,
  receiverActionCreator /**
   *
   * @param {Function} dispatch callback for action dispatch from redux thunk
   * @param {Function} getState callback to get the current store state
   * @return {Promise.<*>}
   */
) => async (dispatch, getState) => {
  // Extract the props `pageBaseURL` & `currentPage` from the redux store
  const { [entity]: { pageBaseURL, currentPage } } = getState();
  const pageURL = `${pageBaseURL}${currentPage}`;
  const response = await fetch(pageURL);
  const { status = 'error', data } = await response.json();

  if (status === 'ok') {
    return dispatch(receiverActionCreator({ data }));
  }

  return dispatch(receiverActionCreator(new Error(`Request failed with status ${status}`)));
};

export { createLazyRequest };
