import { createAction } from 'redux-actions';
import {
  createLazyRequest,
  fetchPagedEntities,
  fetchUrnPathEntityNodes,
  fetchUrnPathEntities
} from 'wherehows-web/actions/entities';
import actionSet from 'wherehows-web/actions/action-set';

/**
 * Set of actions for Flows
 * @type {{REQUEST_PAGED_FLOWS: string, SELECT_PAGED_FLOWS: string, RECEIVE_PAGED_FLOWS: string}}
 */
const ActionTypes = {
  REQUEST_PAGED_FLOWS: actionSet('REQUEST_PAGED_FLOWS'),
  SELECT_PAGED_FLOWS: actionSet('SELECT_PAGED_FLOWS'),
  RECEIVE_PAGED_FLOWS: actionSet('RECEIVE_PAGED_FLOWS'),

  REQUEST_PAGED_URN_FLOWS: actionSet('REQUEST_PAGED_URN_FLOWS'),
  RECEIVE_PAGED_URN_FLOWS: actionSet('RECEIVE_PAGED_URN_FLOWS'),

  REQUEST_FLOWS_NODES: actionSet('REQUEST_FLOWS_NODES'),
  RECEIVE_FLOWS_NODES: actionSet('RECEIVE_FLOWS_NODES')
};

const requestPagedFlows = createAction(ActionTypes.REQUEST_PAGED_FLOWS);

const receivePagedFlows = createAction(
  ActionTypes.RECEIVE_PAGED_FLOWS,
  ({ data }) => data,
  // meta data attached to the ActionTypes.RECEIVE_PAGED_FLOWS action
  () => ({ receivedAt: Date.now() })
);

const requestPagedUrnFlows = createAction(ActionTypes.REQUEST_PAGED_URN_FLOWS);

const receivePagedUrnFlows = createAction(
  ActionTypes.RECEIVE_PAGED_URN_FLOWS,
  ({ data }) => data,
  () => ({ receivedAt: Date.now() })
);

const requestFlowsNodes = createAction(ActionTypes.REQUEST_FLOWS_NODES);
const receiveFlowsNodes = createAction(
  ActionTypes.RECEIVE_FLOWS_NODES,
  response => response,
  // meta data attached to the ActionTypes.RECEIVE_PAGED_FLOWS action
  () => ({ receivedAt: Date.now() })
);

// async action/thunk creator for ActionTypes.REQUEST_PAGED_FLOWS
// TODO: Is this redundant since we can use name without a name queryParam supplied
const lazyRequestPagedFlows = createLazyRequest(requestPagedFlows, receivePagedFlows, fetchPagedEntities('flows'));

const lazyRequestFlowsNodes = createLazyRequest(requestFlowsNodes, receiveFlowsNodes, fetchUrnPathEntityNodes('flows'));

const lazyRequestPagedUrnApplicationFlows = createLazyRequest(
  requestPagedUrnFlows,
  receivePagedUrnFlows,
  fetchUrnPathEntities('flows')
);

export { ActionTypes, lazyRequestPagedFlows, lazyRequestFlowsNodes, lazyRequestPagedUrnApplicationFlows };
