import Component from '@ember/component';
import { get, set, computed } from '@ember/object';
import { task, TaskInstance } from 'ember-concurrency';
import ComputedProperty from '@ember/object/computed';
import { IChartDatum } from 'wherehows-web/typings/app/visualization/charts';

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
    /**
     * Triggered when the user clicks on one of the bars in the summary charts child component, will trigger
     * a filter for whatever bar they select, unless it already is one in which case we will remove the filter
     * @param this - Explicit this declaration for typescript
     * @param filterDatum - Passed in to the action by the child component, contains the tag to be filtered for
     */
    onFilterSelect(this: DatasetHealthContainer, filterDatum: IChartDatum): void {
      const currentFilters = get(this, 'currentFilters');
      const filterName = filterDatum.name;

      if (currentFilters.has(filterName)) {
        currentFilters.delete(filterName);
      } else {
        // This strange little logic here makes sure we have only one active filter at a time. Taking this away will
        // essentially give us multiple filter support (if we decide to make such a UI for it)
        currentFilters.clear();
        currentFilters.add(filterName);
      }

      this.notifyPropertyChange('currentFilters');
    }
  };

  // Mock data for testing demo purposes, to be deleted once we have actual data and further development
  testSeries = [{ name: 'Test1', value: 10 }, { name: 'Test2', value: 5 }, { name: 'Test3', value: 3 }];
  fakeCategories: ComputedProperty<Array<IChartDatum>> = computed('currentFilters', function(
    this: DatasetHealthContainer
  ): Array<IChartDatum> {
    const baseCategories = [{ name: 'Compliance', value: 60 }, { name: 'Ownership', value: 40 }];
    const currentFilters = get(this, 'currentFilters');
    const hasFilters = Array.from(currentFilters).length > 0;

    return baseCategories.map(datum => ({ ...datum, isFaded: hasFilters && !currentFilters.has(datum.name) }));
  });

  fakeSeverity: ComputedProperty<Array<IChartDatum>> = computed('currentFilters', function(
    this: DatasetHealthContainer
  ): Array<IChartDatum> {
    const baseSeverities = [
      { name: 'Minor', value: 50, customColorClass: 'severity-chart__bar--minor' },
      { name: 'Warning', value: 30, customColorClass: 'severity-chart__bar--warning' },
      { name: 'Critical', value: 25, customColorClass: 'severity-chart__bar--critical' }
    ];
    const currentFilters = get(this, 'currentFilters');
    const hasFilters = Array.from(currentFilters).length > 0;

    return baseSeverities.map(datum => ({ ...datum, isFaded: hasFilters && !currentFilters.has(datum.name) }));
  });
}
