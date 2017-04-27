import Ember from 'ember';
import connect from 'ember-redux/components/connect';

const { Component } = Ember;

/**
 * Selector function that takes a Redux Store to extract
 *   state props for the browser-rail
 * @return {Object} mapping of computed props to state
 */
const stateToComputed = state => {
  // Extracts the current entity active in the browse view
  const { browseEntity: { currentEntity = '' } = {} } = state;
  // Retrieves properties for the current entity from the state tree
  let { browseEntity: { [currentEntity]: { query: { urn } } } } = state;
  // Default urn to null, which represents the top-level parent
  urn = urn || null;

  // Read the list of ids child entity ids associated with the urn
  const { [currentEntity]: { byUrn: { [urn]: childIds = [] } } } = state;
  // Read the list of entities, stored in the byId property
  const { [currentEntity]: { byId: entities } } = state;
  /**
   * Takes the currentEntity which is plural and strips the trailing `s` and appends
   *   the entity sub route
   * @type {string}
   */
  const entityRoute = `${currentEntity}.${currentEntity.slice(0, -1)}`;

  return {
    currentEntity,
    entityRoute,
    entities: childIds.map(id => entities[id]) // Extract out the intersection of childIds from the entity map
  };
};

export default connect(stateToComputed)(Component.extend({}));
