import Ember from 'ember';
import connect from 'ember-redux/components/connect';

const { Component } = Ember;

/**
 * A Selector function that takes the Redux Store and applies
 *   store state to props
 * @param {Object} datasets is the slice of the store containing datasets
 *   and related state
 * @return {{datasets: (any[]|Array), isFetching: boolean}}
 */
const stateToComputed = ({ datasets }) => {
  const { byPage, byId, currentPage, isFetching = false } = datasets;
  // List of datasets for the current Page
  const pagedDatasetIds = byPage[currentPage] || [];

  return {
    // Takes the normalized list of ids and maps to dataset objects
    datasets: pagedDatasetIds.map(datasetId => byId[datasetId]),
    isFetching
  };
};

export default connect(stateToComputed)(Component.extend({}));
