import { createAction } from 'redux-actions';
import { createLazyRequest, fetchPagedEntities } from 'wherehows-web/actions/entities';
import actionSet from 'wherehows-web/actions/action-set';

/**
 * Set of actions for Flows
 * @type {{REQUEST_PAGED_FLOWS: string, SELECT_PAGED_FLOWS: string, RECEIVE_PAGED_FLOWS: string}}
 */
const ActionTypes = {
  REQUEST_PAGED_FLOWS: actionSet('REQUEST_PAGED_FLOWS'),
  SELECT_PAGED_FLOWS: actionSet('SELECT_PAGED_FLOWS'),
  RECEIVE_PAGED_FLOWS: actionSet('RECEIVE_PAGED_FLOWS')
};

const requestPagedFlows = createAction(ActionTypes.REQUEST_PAGED_FLOWS);

const receivePagedFlows = createAction(
  ActionTypes.RECEIVE_PAGED_FLOWS,
  ({ data }) => data,
  // meta data attached to the ActionTypes.RECEIVE_PAGED_FLOWS action
  () => ({ receivedAt: Date.now() })
);

// async action/thunk creator for ActionTypes.REQUEST_PAGED_FLOWS
const lazyRequestPagedFlows = createLazyRequest(requestPagedFlows, receivePagedFlows, fetchPagedEntities('flows'));

export { ActionTypes, lazyRequestPagedFlows };
