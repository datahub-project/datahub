import { ActionTypes } from 'wherehows-web/actions/datasets';
import _ from 'lodash';

const { merge, union } = _;
/**
 * Initial datasets slice
 * @type {{isFetchingDatasets: boolean, datasetsPage: null, datasetsPageBaseURL: string, datasetsById: {}, datasetsByPage: {}}}
 */
const initialState = {
  isFetchingDatasets: false,
  datasetsPage: null,
  datasetsPageBaseURL: '',

  datasetsById: {},

  datasetsByPage: {}
};

/**
 * Creates a map of datasetIds to dataset entity
 * @param {Array} datasets = [] list of datasets to map to ids
 */
const mapDatasetsToIds = (datasets = []) =>
  datasets.reduce(
    (idMap, dataset) => {
      idMap[dataset.id] = dataset;
      return idMap;
    },
    {}
  );
/**
 * Merges previous datasets and a new map of ids to datasets into a new map
 * @param {Object} prevDatasets current list of ids mapped to datasets
 * @param {Array} datasets list of received datasets
 */
const datasetsToIds = (prevDatasets, { datasets }) => merge({}, prevDatasets, mapDatasetsToIds(datasets));

/**
 * Returns a new map of page numbers to datasetIds
 * @param {Object} pagedDatasets
 * @param {Array} datasets list of received datasets
 * @param {Number|String} page the page that contains the returned list of datasets
 * @return {*}
 */
const datasetsToPage = (pagedDatasets = {}, { datasets, page }) => {
  return Object.assign({}, pagedDatasets, {
    [page]: union(pagedDatasets[page] || [], datasets.mapBy('id'))
  });
};

/**
 * Reduces the store state for datasets
 * @param {Object} state = initialState the slice of the state object representing datasets
 * @param {Object} action Flux Standard Action representing the action to be preformed on the state
 * @prop {String} action.type actionType
 * @return {*}
 */
export default (state = initialState, action) => {
  switch (action.type) {
    case ActionTypes.RECEIVE_PAGED_DATASETS:
      return Object.assign({}, state, {
        datasetsById: datasetsToIds(state.datasetsById, action.payload),
        datasetsByPage: datasetsToPage(state.datasetsByPage, action.payload),
        isFetchingDatasets: false
      });

    case ActionTypes.SELECT_PAGED_DATASETS:
    case ActionTypes.REQUEST_PAGED_DATASETS:
      return Object.assign({}, state, {
        datasetsPage: action.payload.page,
        datasetsPageBaseURL: action.payload.datasetsPageBaseURL,
        isFetchingDatasets: true
      });

    default:
      return state;
  }
};
