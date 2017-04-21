import { createAction } from 'redux-actions';
import { createLazyRequest } from 'wherehows-web/actions/entities';
import actionSet from 'wherehows-web/actions/action-set';

/**
 * Set of actions for datasets
 * @type {{REQUEST_PAGED_DATASETS: string, SELECT_PAGED_DATASETS: string, RECEIVE_PAGED_DATASETS: string}}
 */
const ActionTypes = {
  REQUEST_PAGED_DATASETS: actionSet('REQUEST_PAGED_DATASETS'),
  SELECT_PAGED_DATASETS: actionSet('SELECT_PAGED_DATASETS'),
  RECEIVE_PAGED_DATASETS: actionSet('RECEIVE_PAGED_DATASETS')
};

const requestPagedDatasets = createAction(ActionTypes.REQUEST_PAGED_DATASETS);

const selectPagedDatasets = createAction(ActionTypes.SELECT_PAGED_DATASETS);

const receivePagedDatasets = createAction(
  ActionTypes.RECEIVE_PAGED_DATASETS,
  ({ data }) => data,
  // meta data attached to the ActionTypes.RECEIVE_PAGED_DATASETS action
  () => ({ receivedAt: Date.now() })
);

// Async action creator for ActionTypes.REQUEST_PAGED_DATASETS
const lazyRequestPagedDatasets = createLazyRequest('datasets', requestPagedDatasets, receivePagedDatasets);

// Async action creator for ActionTypes.SELECT_PAGED_DATASETS
const lazySelectPagedDatasets = createLazyRequest('datasets', selectPagedDatasets, receivePagedDatasets);

export { ActionTypes, lazyRequestPagedDatasets, lazySelectPagedDatasets, receivePagedDatasets };
