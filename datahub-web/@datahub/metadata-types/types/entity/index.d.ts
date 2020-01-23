/**
 * Common fields that apply to all entities
 * @export
 * @interface IBaseEntity
 */
export interface IBaseEntity {
  // The resource name for the entity
  urn?: string;
  // The identifier for a dataset entity, available if the inheriting entity is a Dataset
  uri?: string;
  // Whether the entity has been removed or not, removed means a soft deletion
  removed: boolean;
}
