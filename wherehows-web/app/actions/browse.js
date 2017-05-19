import actionSet from 'wherehows-web/actions/action-set';
import { createAction } from 'redux-actions';
import { lazyRequestPagedFlows, ActionTypes as FlowsActions } from 'wherehows-web/actions/flows';
import { lazyRequestPagedMetrics, ActionTypes as MetricsActions } from 'wherehows-web/actions/metrics';
import { lazyRequestPagedDatasets, ActionTypes as DatasetsActions } from 'wherehows-web/actions/datasets';

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
 * @param {String} entity the entity currently being viewed
 * @param {Object.<String>} urls for the entities being viewed
 */
const asyncRequestBrowseData = (page, entity, urls) =>
  /**
   * Async Thunk
   * @param {Function} dispatch
   * @return {Promise.<*>}
   */
  async function(dispatch) {
    dispatch(requestBrowseData({ entity }));

    try {
      const thunks = await Promise.all([
        dispatch(lazyRequestPagedFlows({ baseURL: urls.flows, query: { page } })),
        dispatch(lazyRequestPagedMetrics({ baseURL: urls.metrics, query: { page } })),
        dispatch(lazyRequestPagedDatasets({ baseURL: urls.datasets, query: { page } }))
      ]);
      const [...actions] = thunks;

      /**
       * Check that none of the actions has an error flag in FSA
       * @type {boolean|*}
       */
      const isValidBrowseData = actions.every(({ error = false }) => !error);

      // If all requests are successful, apply browse data for each entity
      if (isValidBrowseData) {
        const browseData = actions.reduce((browseData, { payload, type }) => {
          const { count, itemsPerPage } = payload;
          const entity = {
            [FlowsActions.RECEIVE_PAGED_FLOWS]: 'flows',
            [MetricsActions.RECEIVE_PAGED_METRICS]: 'metrics',
            [DatasetsActions.RECEIVE_PAGED_DATASETS]: 'datasets'
          }[type];

          browseData[entity] = {
            count,
            itemsPerPage
          };

          return browseData;
        }, {});

        return dispatch(receiveBrowseData({ browseData }));
      }

      return dispatch(receiveBrowseData(new Error(`An error occurred during the data request`)));
    } catch (e) {
      return dispatch(receiveBrowseData(new Error(`An error occurred during the data request: ${e}`)));
    }
  };

export { ActionTypes, asyncRequestBrowseData };
