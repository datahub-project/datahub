import { createAction } from 'redux-actions';
import {
  createLazyRequest,
  fetchPagedEntities,
  fetchPagedUrnEntities,
  fetchNodes
} from 'wherehows-web/actions/entities';
import actionSet from 'wherehows-web/actions/action-set';

/**
 * Set of actions for datasets
 * @type {{REQUEST_PAGED_DATASETS, SELECT_PAGED_DATASETS, RECEIVE_PAGED_DATASETS, REQUEST_PAGED_URN_DATASETS, RECEIVE_PAGED_URN_DATASETS, REQUEST_DATASET_NODES, RECEIVE_DATASET_NODES}}
 */
const ActionTypes = {
  REQUEST_PAGED_DATASETS: actionSet('REQUEST_PAGED_DATASETS'),
  SELECT_PAGED_DATASETS: actionSet('SELECT_PAGED_DATASETS'),
  RECEIVE_PAGED_DATASETS: actionSet('RECEIVE_PAGED_DATASETS'),

  REQUEST_PAGED_URN_DATASETS: actionSet('REQUEST_PAGED_URN_DATASETS'),
  RECEIVE_PAGED_URN_DATASETS: actionSet('RECEIVE_PAGED_URN_DATASETS'),

  REQUEST_DATASET_NODES: actionSet('REQUEST_DATASET_NODES'),
  RECEIVE_DATASET_NODES: actionSet('RECEIVE_DATASET_NODES')
};

const requestPagedDatasets = createAction(ActionTypes.REQUEST_PAGED_DATASETS);
const receivePagedDatasets = createAction(
  ActionTypes.RECEIVE_PAGED_DATASETS,
  ({ data }) => data,
  // meta data attached to the ActionTypes.RECEIVE_PAGED_DATASETS action
  () => ({ receivedAt: Date.now() })
);

const requestUrnPagedDatasets = createAction(ActionTypes.REQUEST_PAGED_URN_DATASETS);
const receiveUrnPagedDatasets = createAction(
  ActionTypes.RECEIVE_PAGED_URN_DATASETS,
  ({ data }) => data,
  // meta data attached to the ActionTypes.RECEIVE_PAGED_DATASETS action
  () => ({ receivedAt: Date.now() })
);

const requestDatasetNodes = createAction(ActionTypes.REQUEST_DATASET_NODES);
const receiveDatasetNodes = createAction(
  ActionTypes.RECEIVE_DATASET_NODES,
  response => response,
  // meta data attached to the ActionTypes.RECEIVE_PAGED_DATASETS action
  () => ({ receivedAt: Date.now() })
);
// Async action creator for ActionTypes.REQUEST_PAGED_DATASETS
const lazyRequestPagedDatasets = createLazyRequest(
  requestPagedDatasets,
  receivePagedDatasets,
  fetchPagedEntities('datasets')
);

// Async action creator for ActionTypes.REQUEST_PAGED_URN_DATASETS
const lazyRequestUrnPagedDatasets = createLazyRequest(
  requestUrnPagedDatasets,
  receiveUrnPagedDatasets,
  fetchPagedUrnEntities('datasets')
);

// Async action creator for ActionTypes.REQUEST_DATASET_NODES
const lazyRequestDatasetNodes = createLazyRequest(requestDatasetNodes, receiveDatasetNodes, fetchNodes('datasets'));

export { ActionTypes, lazyRequestPagedDatasets, lazyRequestUrnPagedDatasets, lazyRequestDatasetNodes };
