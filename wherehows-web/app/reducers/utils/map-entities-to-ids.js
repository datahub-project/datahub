/**
 * Creates a map of ids to entity
 * @param {Array} entities = [] list of entities to map to ids
 */
export default (entities = []) =>
  entities.reduce((idMap, entity) => {
    idMap[entity.id] = entity;
    return idMap;
  }, {});
