import Component from '@ember/component';
import { get, set, getProperties } from '@ember/object';
import { task, TaskInstance } from 'ember-concurrency';
import { action } from 'ember-decorators/object';
import { IOwner } from 'wherehows-web/typings/api/datasets/owners';
import {
  OwnerType,
  readDatasetOwnersByUrn,
  readDatasetOwnerTypesWithoutConsumer,
  updateDatasetOwnersByUrn
} from 'wherehows-web/utils/api/datasets/owners';

export default class DatasetOwnershipContainer extends Component {
  /**
   * The urn identifier for the dataset
   * @type {string}
   */
  urn: string;

  /**
   * List of owners for the dataset
   * @type {Array<IOwner>}
   */
  owners: Array<IOwner>;

  /**
   * List of types available for a dataset owner
   * @type {Array<OwnerType>}
   */
  ownerTypes: Array<OwnerType>;

  didInsertElement() {
    get(this, 'getContainerDataTask').perform();
  }

  didUpdateAttrs() {
    get(this, 'getContainerDataTask').perform();
  }

  /**
   * An async parent task to group all data tasks for this container component
   * @type {Task<TaskInstance<Promise<any>>, (a?: any) => TaskInstance<TaskInstance<Promise<any>>>>}
   */
  getContainerDataTask = task(function*(this: DatasetOwnershipContainer): IterableIterator<TaskInstance<Promise<any>>> {
    const tasks = Object.values(getProperties(this, ['getDatasetOwnersTask', 'getDatasetOwnerTypesTask']));

    yield* tasks.map(task => task.perform());
  });

  /**
   * Reads the owners for this dataset
   * @type {Task<Promise<Array<IOwner>>, (a?: any) => TaskInstance<Promise<Array<IOwner>>>>}
   */
  getDatasetOwnersTask = task(function*(this: DatasetOwnershipContainer): IterableIterator<Promise<Array<IOwner>>> {
    const owners = yield readDatasetOwnersByUrn(get(this, 'urn'));

    set(this, 'owners', owners);
  });

  /**
   * Reads the owner types available
   * @type {Task<Promise<Array<OwnerType>>, (a?: any) => TaskInstance<Promise<Array<OwnerType>>>>}
   */
  getDatasetOwnerTypesTask = task(function*(
    this: DatasetOwnershipContainer
  ): IterableIterator<Promise<Array<OwnerType>>> {
    const ownerTypes = yield readDatasetOwnerTypesWithoutConsumer();
    set(this, 'ownerTypes', ownerTypes);
  });

  /**
   * Persists the changes to the owners list
   * @param {Array<IOwner>} updatedOwners
   * @return {Promise<void>}
   */
  @action
  saveOwnerChanges(updatedOwners: Array<IOwner>): Promise<void> {
    return updateDatasetOwnersByUrn(get(this, 'urn'), '', updatedOwners);
  }
}
