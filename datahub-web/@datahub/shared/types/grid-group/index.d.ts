/**
 * TODO 11471: Refactor top consumers for internal vs external compatibility
 * The interface for the grid group entity
 * @export
 * @interface IGridGroup
 */
export interface IGridGroupEntity {
  // The resource identity of the entity
  urn: string;
  // The name of the entity after the entity urn prefix
  name: string;
  // A link to the primary client interface where users can perform actions
  // related to the specific grid group entity
  link: string;
}
