import Ember from 'ember';
import connect from 'ember-redux/components/connect';

const { Component } = Ember;
/**
 * Selector function that takes a Redux Store to extract
 *   state props for the browser-view
 * @param {Object} browseData
 * @return {Object}
 */
const stateToComputed = ({ browse: { browseData = {} } = {} }) => ({ browseData });
export default connect(stateToComputed)(Component.extend({}));
