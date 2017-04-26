import Ember from 'ember';
import { createAction } from 'redux-actions';

import actionSet from 'wherehows-web/actions/action-set';
import { lazyRequestUrnPagedDatasets, lazyRequestDatasetNodes } from 'wherehows-web/actions/datasets';

const { debug } = Ember;

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
const asyncRequestNodeList = (params, listURL, { queryParams }) =>
  /**
   * Async thunk
   * @param {Function} dispatch
   * @return {Promise.<*>}
   */
  async function(dispatch) {
    const { entity, page, urn } = params;
    const query = { page, urn };

    dispatch(requestNodeList({ entity, listURL, query, queryParams }));

    try {
      let nodesResult = {}, pagedEntities = {};
      switch (entity) {
        case 'datasets':
          [nodesResult, pagedEntities] = await [
            dispatch(lazyRequestDatasetNodes({ listURL, query })),
            dispatch(lazyRequestUrnPagedDatasets({ query }))
          ];
          break;
        case 'metrics':
        case 'flows':
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

export { ActionTypes, asyncRequestNodeList };
