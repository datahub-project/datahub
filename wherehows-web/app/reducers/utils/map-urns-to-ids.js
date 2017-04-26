/**
 * Creates a map of urn to id
 * @param {Array} entities = [] list of entities
 */
export default (entities = []) =>
  entities.reduce(
    (idMap, {urn, id}) => {
      idMap[urn] = id;
      return idMap;
    },
    {}
  );
