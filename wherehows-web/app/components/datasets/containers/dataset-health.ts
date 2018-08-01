import Component from '@ember/component';
import { get, set } from '@ember/object';
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

  /**
   * Sets the classes for the rendered html element for the component
   * @type {Array<string>}
   */
  classNames = ['dataset-health'];
  /**
   * The current filter is used to set a filter for the table to show only items within a certain category or
   * severity. It is set as a set of strings in order to support the idea of multiple filters in the future,
   * even though our initial version will support only one filter at a time.
   * @type {Array<string>}
   */
  currentFilters: Set<string>;
  constructor() {
    super(...arguments);
    set(this, 'currentFilters', new Set());
  }

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
    /*
     * Triggered when the user clicks on one of the bars in the summary charts child component, will trigger
     * a filter for whatever bar they select, unless it already is one in which case we will remove the filter
     * @param this - Explicit this declaration for typescript
     * @param filterName - Passed in to the action by the child component, contains the tag to be filtered for
     */
    onFilterSelect(this: DatasetHealthContainer, filterName: string): void {
      const currentFilters = get(this, 'currentFilters');
      currentFilters.has(filterName) ? currentFilters.delete(filterName) : currentFilters.add(filterName);
    }
  };

  // Mock data for testing demo purposes, to be deleted once we have actual data and further development
  testSeries = [{ name: 'Test1', value: 10 }, { name: 'Test2', value: 5 }, { name: 'Test3', value: 3 }];
  fakeCategories = [{ name: 'Compliance', value: 60 }, { name: 'Ownership', value: 40 }];
  fakeSeverity = [{ name: 'Minor', value: 50 }, { name: 'Warning', value: 30 }, { name: 'Critical', value: 25 }];
}
