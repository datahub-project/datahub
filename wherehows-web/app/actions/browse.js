import actionSet from 'wherehows-web/actions/action-set';
import { createAction } from 'redux-actions';
import { lazyRequestPagedFlows } from 'wherehows-web/actions/flows';
import { lazyRequestPagedMetrics } from 'wherehows-web/actions/metrics';
import { lazyRequestPagedDatasets } from 'wherehows-web/actions/datasets';

const ActionTypes = {
  REQUEST_BROWSE_DATA: actionSet('REQUEST_BROWSE_DATA'),
  RECEIVE_BROWSE_DATA: actionSet('RECEIVE_BROWSE_DATA')
};

// Caches Flux Standard (FSA) Action Creators
const requestBrowseData = createAction(ActionTypes.REQUEST_BROWSE_DATA);
const receiveBrowseData = createAction(ActionTypes.RECEIVE_BROWSE_DATA);

/**
 * Returns an Async Action Creator sync dispatches `requestBrowseData` delegates to
 *   Flows, Metrics and Datasets thunk creators,
 *   sync dispatches action in `receiveBrowseData` on completion
 * @param {String|Number} page current browse page
 * @param {String} viewingEntity the entity currently being viewed
 * @param {Object.<String>} urls for the entities being viewed
 */
const asyncRequestBrowseData = (page, viewingEntity, urls) =>
  /**
   * Async Thunk
   * @param {Function} dispatch
   * @return {Promise.<*>}
   */
  async function (dispatch) {
    dispatch(requestBrowseData({ viewingEntity }));

    try {
      const thunks = await Promise.all([
        dispatch(lazyRequestPagedFlows(urls.flows, page)),
        dispatch(lazyRequestPagedMetrics(urls.metrics, page)),
        dispatch(lazyRequestPagedDatasets(urls.datasets, page))
      ]);
      const [...actions] = await Promise.all(thunks);

      /**
       * Check that none of the actions has an error flag in FSA
       * @type {boolean|*}
       */
      const isValidBrowseData = actions.every(({ error = false }) => !error);

      if (isValidBrowseData) {
        return dispatch(receiveBrowseData());
      }

      return dispatch(receiveBrowseData(new Error(`An error occurred during the data request`)));
    } catch (e) {
      return dispatch(receiveBrowseData(new Error(`An error occurred during the data request: ${e}`)));
    }
  };

export { ActionTypes, asyncRequestBrowseData };
