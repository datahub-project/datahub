/**
 * Generic metadata model that is associated with an top-level entity
 * @interface IMetadata
 */
export interface IMetadata {
  // Entity this sub resource metadata is associated with
  entityKey: string;
  // Versioning allows to capture evolution of this sub resource
  version?: number;
}
