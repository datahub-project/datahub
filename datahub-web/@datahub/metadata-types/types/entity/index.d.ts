/**
 * Common fields that apply to all entities
 * @export
 * @interface IBaseEntity
 */
export interface IBaseEntity {
  // The resource name for the entity
  urn: string;
  // Whether the entity has been removed or not, removed means a soft deletion
  removed: boolean;
}
