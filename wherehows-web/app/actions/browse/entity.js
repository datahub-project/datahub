import { debug } from '@ember/debug';
import { createAction } from 'redux-actions';

import actionSet from 'wherehows-web/actions/action-set';
import { lazyRequestUrnPagedDatasets, lazyRequestDatasetNodes } from 'wherehows-web/actions/datasets';
import { lazyRequestNamedPagedMetrics, lazyRequestMetricNodes } from 'wherehows-web/actions/metrics';
import { lazyRequestFlowsNodes, lazyRequestPagedUrnApplicationFlows } from 'wherehows-web/actions/flows';

const ActionTypes = {
  REQUEST_NODE_LIST: actionSet('REQUEST_NODE_LIST'),
  RECEIVE_NODE_LIST: actionSet('RECEIVE_NODE_LIST')
};

/**
 * Start and end wrapping actions for request an entities node list
 */
const requestNodeList = createAction(ActionTypes.REQUEST_NODE_LIST);
const receiveNodeList = createAction(ActionTypes.RECEIVE_NODE_LIST);

/**
 * Wraps action sequence for request node list and related child entities for urn
 * @param {Object} params
 * @param {String} listURL
 * @param {Array} queryParams current list of query parameters for the Ember route
 */
const asyncRequestEntityQueryData = (params, listURL, { queryParamsKeys: queryParams }) =>
  /**
   * Async thunk
   * @param {Function} dispatch
   * @return {Promise.<*>}
   */
  async function(dispatch) {
    const { entity, page, urn, name } = params;
    // Extract relevant query parameters into query object
    const query = { page, urn, name };

    dispatch(requestNodeList({ entity, listURL, query, queryParams }));

    // For each entity fetch the list of nodes and the actual entities for the given query
    try {
      let nodesResult = {},
        pagedEntities = {};
      switch (entity) {
        case 'datasets':
          [nodesResult, pagedEntities] = await Promise.all([
            dispatch(lazyRequestDatasetNodes({ listURL, query })),
            dispatch(lazyRequestUrnPagedDatasets({ query }))
          ]);
          break;
        case 'metrics':
          [nodesResult, pagedEntities] = await Promise.all([
            dispatch(lazyRequestMetricNodes({ listURL, query })),
            dispatch(lazyRequestNamedPagedMetrics({ query }))
          ]);
          break;
        case 'flows':
          [nodesResult, pagedEntities] = await Promise.all([
            dispatch(lazyRequestFlowsNodes({ listURL, query })),
            dispatch(lazyRequestPagedUrnApplicationFlows({ query }))
          ]);
          break;
        default:
          return;
      }
      // If there are no errors on the action payloads, dispatch `receiveNodeList` action creator with nodesResult
      //   and exit handler
      if (!pagedEntities.error || !nodesResult.error) {
        return dispatch(receiveNodeList({ nodesResult, entity }));
      }

      return dispatch(
        receiveNodeList(
          new Error(`An error occurred requesting data. list: ${nodesResult.status} entities: ${pagedEntities.status}`)
        )
      );
    } catch (e) {
      debug(e);
      return dispatch(receiveNodeList(new Error(`An error occurred requesting data ${e}`)));
    }
  };

export { ActionTypes, asyncRequestEntityQueryData };
