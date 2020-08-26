import { DataModelEntityInstance, DataModelName } from '@datahub/data-models/constants/entity';

/**
 * Expresses the form of a getter function for a data model entity when given a urn and entity type
 */
export type DataModelsRelationshipGetter<K extends DataModelEntityInstance> = <T extends string | Array<string>>(
  urns: T
) => (T extends string ? K : Array<K>) | undefined;

/**
 * Creates a custom object to provide the interface for the Objects we are actually working with.
 * Since we are messing with decorators and prototypes under the hood, typescript won't actually
 * know such a property exists on the object interface
 */
export interface IRelationshipDecoratedClassPrototype {
  __relationshipGetters: Partial<Record<DataModelName, DataModelsRelationshipGetter<DataModelEntityInstance>>>;
  __relationships: Set<DataModelName>;
  [key: string]: unknown;
}
