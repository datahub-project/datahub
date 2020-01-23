import { BaseEntity } from '@datahub/data-models/entity/base-entity';
import { IBaseEntity } from '@datahub/metadata-types/types/entity';
import { set } from '@ember/object';

/**
 * Defines a local alias for the Entity type that should be created by the factory
 * @alias {BaseEntity<IBaseEntity & { urn: string }>}
 */
export type DataModelEntityInstance = BaseEntity<IBaseEntity>;

/**
 * Properties expected to be provided when a new Entity is created
 * @interface IEntityConstructorParams
 */
interface IEntityConstructorParams {
  urn: string;
}

/**
 * Entity factory function receives a constructor Entity type to create, and then arguments required for the
 * constructor in a curried async function
 *
 * An Entity instance may be created by invoking new directly on the constructor, however, the consumer will also have to
 * invoke async methods on the instance to reify the underlying entity and related data such as snapshots.
 * Using the factory asynchronously abstracts these operations
 *
 * Usage:
 * The following are equivalent
 *
import { FeatureEntity } from '@datahub/data-models/entity/feature/feature-entity';

 async function caseA(urn: string) {
  const myFeature = await createEntity(FeatureEntity)({ urn });
  const isFeatureRemoved = myFeature.removed;
  ...
}

async function caseB(urn: string) {
  const myFeature = new FeatureEntity(urn);
  await myFeature.readEntity;

  const isFeatureRemoved = myFeature.removed;
  ...
}
 *
 * The factory returns a curried function that can be repeatedly used to create entities repeatedly,
 * passing only the parameters that differ from one instance to another
 * const createFeatureEntity = createEntity(FeatureEntity);
 * const entityA = await createFeatureEntity({urn: 'urnA'});
 * const entityB = await createFeatureEntity({urn: 'urnB'});
 *
 * or
 * \[urn1, urn2, ...urnN].map(createEntity(FeatureEntity))
 *
 * @template E the Entity to be created
 * @param {new (urn: string) => E} entityConstructor Entity constructor function
 */
export const createEntity = <E extends DataModelEntityInstance>(
  entityConstructor: new (urn: string) => E
): ((p: IEntityConstructorParams) => Promise<E>) => async ({ urn }: IEntityConstructorParams): Promise<E> => {
  const entity = new entityConstructor(urn);
  // Post instantiation, request the underlying Entity instance and Snapshot
  // and set the related instance attributes with references to the value
  set(entity, 'entity', await entity.readEntity);
  set(entity, 'snapshot', await entity.readSnapshot);

  return entity;
};
