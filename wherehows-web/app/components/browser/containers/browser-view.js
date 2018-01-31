import Component from '@ember/component';
import { connect } from 'ember-redux';

const entities = ['datasets', 'metrics', 'flows']; // hardcoded here to maintain the sort order
/**
 * Selector function that takes a Redux Store to extract
 *   state props for the browser-view
 * @param {Object} browseData
 * @return {Object}
 */
const stateToComputed = ({ browse: { browseData = {} } = {} }) => ({
  browseData: entities.map(browseDatum =>
    Object.assign(
      { entity: browseDatum }, // assigns key name to resulting object
      browseData[browseDatum]
    )
  )
});
export default connect(stateToComputed)(Component.extend({}));
