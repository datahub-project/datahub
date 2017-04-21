import { createAction } from 'redux-actions';
import { createLazyRequest } from 'wherehows-web/actions/entities';
import actionSet from 'wherehows-web/actions/action-set';

/**
 * Set of actions for Metrics
 * @type {{REQUEST_PAGED_METRICS: string, SELECT_PAGED_METRICS: string, RECEIVE_PAGED_METRICS: string}}
 */
const ActionTypes = {
  REQUEST_PAGED_METRICS: actionSet('REQUEST_PAGED_METRICS'),
  SELECT_PAGED_METRICS: actionSet('SELECT_PAGED_METRICS'),
  RECEIVE_PAGED_METRICS: actionSet('RECEIVE_PAGED_METRICS')
};

const requestPagedMetrics = createAction(ActionTypes.REQUEST_PAGED_METRICS);

const selectPagedMetrics = createAction(ActionTypes.SELECT_PAGED_METRICS);

const receivePagedMetrics = createAction(
  ActionTypes.RECEIVE_PAGED_METRICS,
  ({ data }) => data,
  // meta data attached to the ActionTypes.RECEIVE_PAGED_METRICS action
  () => ({ receivedAt: Date.now() })
);

// async action/thunk creator for ActionTypes.REQUEST_PAGED_METRICS
const lazyRequestPagedMetrics = createLazyRequest('metrics', requestPagedMetrics, receivePagedMetrics);

// async action/thunk creator for ActionTypes.SELECT_PAGED_METRICS
const lazySelectPagedMetrics = createLazyRequest('metrics', selectPagedMetrics, receivePagedMetrics);

export { ActionTypes, lazyRequestPagedMetrics, lazySelectPagedMetrics, receivePagedMetrics };
