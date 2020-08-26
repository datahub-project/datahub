import { DatasetEntity } from '@datahub/data-models/entity/dataset/dataset-entity';

// TODO: [META-9851] Temporary home for this function for integrated dev/demo, should be moved to test/dummy folder
/**
 * Models a DatasetEntity-like class that we can mess with for mock data purposes in the dummy app.
 * This way, we don't need a full implementation for a mock entity in order to view it in our dummy
 * environment
 */
export class FakeDatasetEntity extends DatasetEntity {
  savedName = '';

  /**
   * Creates a blank name field for the object that we can fill in for mock data
   */
  get name(): string {
    return this.savedName;
  }

  set name(name: string) {
    this.savedName = name;
  }
}
