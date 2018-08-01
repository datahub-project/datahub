import Component from '@ember/component';
import { get } from '@ember/object';
import { task, TaskInstance } from 'ember-concurrency';

/**
 * This is the container component for the dataset health tab. It should contain the health bar graphs and a table
 * depicting the detailed health scores. Aside from fetching the data, it also handles click interactions between
 * the graphs and the table in terms of filtering and displaying of data
 */
export default class DatasetHealthContainer extends Component {
  /**
   * The urn identifier for the dataset
   * @type {string}
   */
  urn: string;

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
  getContainerDataTask = task(function*(this: DatasetHealthContainer): IterableIterator<TaskInstance<Promise<any>>> {
    // Do something in the future
  });

  actions = {
    onFilterSelect(): void {
      // Change filter so that table can only show a certain category or severity
    }
  };

  // Mock data for testing demo purposes, to be deleted once we have actual data and further development
  testSeries = [{ name: 'Test1', value: 10 }, { name: 'Test2', value: 5 }, { name: 'Test3', value: 3 }];
}
