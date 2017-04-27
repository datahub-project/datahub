import Ember from 'ember';
import connect from 'ember-redux/components/connect';

const { Component } = Ember;
/**
 * Selector function that takes a Redux Store to extract
 *   state props for the browser-view
 * @param {Object} browseData
 * @return {Object}
 */
const stateToComputed = ({ browse: { browseData = {} } = {} }) => ({
  browseData: Object.keys(browseData)
    .sort() // Datasets implicitly comes first [datasets, flows, metrics]
    .map(browseDatum =>
      Object.assign(
        { entity: browseDatum }, // assigns key name to resulting object
        browseData[browseDatum]
      )
    )
});
export default connect(stateToComputed)(Component.extend({}));
