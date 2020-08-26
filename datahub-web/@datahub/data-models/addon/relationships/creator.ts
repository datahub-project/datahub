import { DataModelName, IDataModelEntity, DataModelEntityInstance } from '@datahub/data-models/constants/entity';
import {
  IRelationshipDecoratedClassPrototype,
  DataModelsRelationshipGetter
} from '@datahub/data-models/types/relationships/decorator-types';
import { isArray } from '@ember/array';

/**
 * Because we're dealing with a prototype that we don't want to expose to the typescript interface
 * for these class objects, this custom interface is used in place of those
 */
interface IRelationshipDecoratorModifiedEntity {
  prototype: IRelationshipDecoratedClassPrototype;
}

/**
 * Shortcut typing for determining whether we have one instance of an entity or an array of instances
 */
type OneOrMany<T, K> = T extends string ? K : Array<K>;

/**
 * Given an entity type and function to create an instance as a callback, we create a getter
 * function that, for a urn or array of urns, returns the instances for those urns
 * @param {DataModelName} entityType - the name identifier for the entity type for which we want
 *  to provide an instance
 * @param {Function} instanceCreator - a function that is given to fetch an instance of the entity
 *  type we have specified
 */
const createRelationshipGetter = <T extends DataModelName>(
  entityType: T,
  instanceCreator: (entityType: DataModelName, urn: string) => DataModelEntityInstance
): DataModelsRelationshipGetter<InstanceType<IDataModelEntity[T]>> => {
  const getter = <M extends string | Array<string>>(
    urns: M
  ): OneOrMany<M, InstanceType<IDataModelEntity[T]>> | undefined => {
    if (typeof urns === 'string') {
      return instanceCreator(entityType, urns) as OneOrMany<M, InstanceType<IDataModelEntity[T]>>;
    } else if (isArray(urns)) {
      return (urns as Array<string>).map(
        (urn): DataModelEntityInstance => instanceCreator(entityType, urn)
      ) as OneOrMany<M, InstanceType<IDataModelEntity[T]>>;
    }

    return;
  };

  return getter;
};

/**
 * Given an entity class object (i.e. PersonEntity), return the metadata for the relationships
 * for that class, given by the use of the @relationship decorator
 * @param {IRelationshipDecoratorModifiedEntity} entityClass - the entity class object for a
 *  specific entity type
 */
const readRelationshipDecoratorMetadata = (entityClass: IRelationshipDecoratorModifiedEntity): Array<DataModelName> =>
  Array.from(entityClass.prototype.__relationships || new Set());

/**
 * Given a class object that has been modified with the @relationship decorator, and a creator
 * function, modifies that class object so that the relationship getters can be attached to the
 * class and be accessible by the functions given in the decorator logic
 * @param {unknown} entityClass - expected to be a modified DataModelEntity class, typed as unknown
 *  for the noted reason below
 * @param {Function} instanceCreator - callback function that creates an instance of an entity
 *  type when given the type and a corresponding urn
 */
// Note: We type entityClass as unknown as we've hidden the prototype modifications from typescript
// so that these properties are not accidentally accessed outside of the relationship decorator and
// creator process
export const assignRelationshipGettersToClassObject = (
  entityClass: Function,
  instanceCreator: (entityType: DataModelName, urn: string) => DataModelEntityInstance
): void => {
  // Even though our parameter lacks proper type safety, we assert the type we are working with
  // from here on out for some limited type safety
  const klass = entityClass as IRelationshipDecoratorModifiedEntity;
  const relatedEntities = readRelationshipDecoratorMetadata(klass);

  relatedEntities.forEach((entityType): void => {
    klass.prototype.__relationshipGetters = {
      ...(klass.prototype.__relationshipGetters || {}),
      [entityType]: createRelationshipGetter(entityType, instanceCreator)
    };
  });
};
