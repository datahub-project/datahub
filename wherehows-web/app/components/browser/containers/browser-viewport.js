import Component from '@ember/component';
import { connect } from 'ember-redux';

/**
 * Extract the childIds for an entity under a specific category
 * @param entity
 * @param state
 */
const getChildIds = (entity, state = {}) => query =>
  ({
    get datasets() {
      return state[entity].byUrn[query];
    },
    get metrics() {
      return state[entity].byName[query];
    },
    get flows() {
      // Flows are retrieved by name as well
      return this.datasets;
    }
  }[entity]);

/**
 * Selector function that takes a Redux Store to extract
 *   state props for the browser-rail
 * @return {Object} mapping of computed props to state
 */
const stateToComputed = state => {
  // Extracts the current entity active in the browse view
  const { browseEntity: { currentEntity = '' } = {} } = state;
  // Retrieves properties for the current entity from the state tree
  const { browseEntity: { [currentEntity]: { query: { urn, name } } } } = state;
  // Default urn to null, which represents the top-level parent

  const query =
    {
      datasets: urn,
      metrics: name,
      flows: urn
    }[currentEntity] || null;
  // Read the list of ids child entity ids associated with the urn
  const childIds = getChildIds(currentEntity, state)(query) || [];
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
