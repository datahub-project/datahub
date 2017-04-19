import fetch from 'ember-network/fetch';
import { createAction } from 'redux-actions';
import ActionSet from 'wherehows-web/actions/action-set';

/**
 * Set of actions for datasets
 * @type {{REQUEST_PAGED_DATASETS: string, SELECT_PAGED_DATASETS: string, RECEIVE_PAGED_DATASETS: string}}
 */
const ActionTypes = {
  REQUEST_PAGED_DATASETS: ActionSet('REQUEST_PAGED_DATASETS'),
  SELECT_PAGED_DATASETS: ActionSet('SELECT_PAGED_DATASETS'),
  RECEIVE_PAGED_DATASETS: ActionSet('RECEIVE_PAGED_DATASETS')
};

const requestPagedDatasets = createAction(ActionTypes.REQUEST_PAGED_DATASETS);

const selectPagedDatasets = createAction(ActionTypes.SELECT_PAGED_DATASETS);

const receivePagedDatasets = createAction(
  ActionTypes.RECEIVE_PAGED_DATASETS,
  ({ data }) => data,
  // meta data attached to the ActionTypes.RECEIVE_PAGED_DATASETS action
  () => ({ receivedAt: Date.now() })
);

/**
 * Async action creator for ActionTypes.REQUEST_PAGED_DATASETS
 * @param {String} datasetsPageBaseURL base url for paged datasets
 * @param {Number} page number of the page to fetch datasets for
 */
const asyncRequestPagedDatasets = (datasetsPageBaseURL, page) =>
  function (dispatch) {
    dispatch(requestPagedDatasets({ datasetsPageBaseURL, page }));
    return asyncReceivePagedDatasets(...arguments);
  };

/**
 * Async action creator for ActionTypes.SELECT_PAGED_DATASETS
 * @param {String} datasetsPageBaseURL base url for paged datasets
 * @param {Number} page number of the page to fetch datasets for
 */
const asyncSelectPagedDatasets = (datasetsPageBaseURL, page) =>
  function (dispatch) {
    dispatch(selectPagedDatasets({ datasetsPageBaseURL, page }));
    return asyncReceivePagedDatasets(...arguments);
  };

/**
 * Async action creator for ActionTypes.RECEIVE_PAGED_DATASETS, fetches a list of datasets at the page in the
 *   current state
 * @param {Function} dispatch function to indicate a store update
 * @param {Function} getState
 * @return {Promise.<Promise.<TResult>|Promise<V, X>>}
 */
const asyncReceivePagedDatasets = async (dispatch, getState) => {
  const { datasets: { datasetsPageBaseURL, datasetsPage } } = getState();
  const datasetsPageURL = `${datasetsPageBaseURL}${datasetsPage}`;
  const response = await fetch(datasetsPageURL);
  const { status = 'error', data } = await response.json();

  // If status returns with 'ok', dispatch action without an error flag
  if (status === 'ok') {
    return dispatch(receivePagedDatasets({ data }));
  }

  // TODO: DSS-6929 Handle error case, FSA still sends action with payload
  return dispatch(receivePagedDatasets(new Error(`Request failed with status ${status}`)));
};

export { ActionTypes, asyncRequestPagedDatasets, asyncSelectPagedDatasets };
