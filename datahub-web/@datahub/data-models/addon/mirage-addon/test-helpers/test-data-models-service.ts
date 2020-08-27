import { TestPersonEntity } from '@datahub/data-models/mirage-addon/test-helpers/test-entities/test-person-entity';
import {
  IDataModelEntity,
  DataModelEntity,
  DataModelName,
  DataModelEntityInstance
} from '@datahub/data-models/constants/entity';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import DataModelsService from '@datahub/data-models/services/data-models';

/**
 * Maps DataModelEntity to our test entities instead where applicable
 */
interface ITestDataModelEntity extends IDataModelEntity {
  [PersonEntity.displayName]: typeof TestPersonEntity;
}

/**
 * Overrides the map of DataModelEntity with our own mapping to test versions of the entities
 * (where necessary)
 */
export const TestDataModelEntity: ITestDataModelEntity = {
  ...DataModelEntity,
  [PersonEntity.displayName]: TestPersonEntity
};

/**
 * Because the open source data models entities can sometimes be too generic, we will want to be
 * able to provide some implementation to the class definitions in order to test certain
 * behaviors that rely on those implementations. In order to do so, we create "test entities" that
 * extend from the open source versions of the entities that provide these implementations. These
 * test entities can also provide an example of how an adopter of DataHub can define their own
 * entity implementations.
 *
 * Since we have these entities, the data models service sometimes needs to be mapped to the test
 * entities instead of the open source entities. We create this extended service to apply that map
 * instead and can be stubbed in place of the regular service when needed
 */
export default class TestDataModelsService extends DataModelsService {
  /**
   * Overrides dataModelEntitiesMapping with a mapping to our test entities instead
   */
  dataModelEntitiesMap = TestDataModelEntity;

  /**
   * Overrides getModel with the map to our test entities instead
   */
  getModel<K extends DataModelName>(modelKey: K): typeof TestDataModelEntity[K] {
    return TestDataModelEntity[modelKey];
  }

  /**
   * Overrides createInstance with our typings for testing instances
   */
  createInstance<K extends DataModelName>(
    modelKey: K,
    urn: string
  ): Promise<InstanceType<typeof TestDataModelEntity[K]>> {
    return super.createInstance(modelKey as DataModelName, urn) as Promise<InstanceType<typeof TestDataModelEntity[K]>>;
  }

  /**
   * Overrides open source generic method wih our specific test entity typings
   */
  createPartialInstance<K extends DataModelName>(
    modelName: K,
    data: DataModelEntityInstance['entity'] | string
  ): InstanceType<typeof TestDataModelEntity[K]> {
    return super.createPartialInstance(
      modelName as DataModelName,
      data as DataModelEntityInstance['entity']
    ) as InstanceType<typeof TestDataModelEntity[K]>;
  }
}
