import Component from '@ember/component';
import { get, computed, setProperties, getProperties } from '@ember/object';
import { task } from 'ember-concurrency';
import ComputedProperty from '@ember/object/computed';
import { IChartDatum } from 'wherehows-web/typings/app/visualization/charts';
import { IHealthScore, IDatasetHealth } from 'wherehows-web/typings/api/datasets/health';
import { healthCategories, healthSeverity, healthDetail } from 'wherehows-web/constants/data/temp-mock/health';
import { readDatasetHealthByUrn } from 'wherehows-web/utils/api/datasets/health';

/**
 * Used for the dataset health tab, represents the fieldnames for the health score table
 */
export enum HealthDataFields {
  category = 'Category',
  severity = 'Severity',
  description = 'Description',
  score = 'Score'
}

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
   * The current filter for the category chart. Clicking on a bar in the chart changes this value and will cause
   * the table component to filter out certain rows. Since we only allow one filter at a time, only this or
   * currentSeverityFilter should have a truthy value at any given point.
   * @type {string}
   */
  currentCategoryFilter = '';

  /**
   * The current filter for the severity chart. Clicking on a bar in the chart changes this value and will cause
   * the table component to filter out certain rows. Since we only allow one filter at a time, only this or
   * currentCategoryFilter should have a truthy value at any given point.
   * @type {string}
   */
  currentSeverityFilter = '';

  /**
   * Raw fetched data for the category metrics
   * @type {Array<pending>}
   */
  categoryMetrics: Array<IChartDatum> = [];

  /**
   * Raw fetched data for the category metrics
   * @type {Array<pending>}
   */
  severityMetrics: Array<IChartDatum> = [];

  /**
   * Fetched data for the health score detailed data.
   * @type {Array<IHealthScore>}
   */
  tableData: Array<IHealthScore> = [];

  /**
   * Modified categoryMetrics to add properties that will help us render our actual charts without modifying the original
   * data
   * @type {ComputedProperty<Array<IChartDatum>>}
   */
  renderedCategories: ComputedProperty<Array<IChartDatum>> = computed(
    'categoryMetrics',
    'currentCategoryFilter',
    function(this: DatasetHealthContainer): Array<IChartDatum> {
      const { categoryMetrics, currentCategoryFilter } = getProperties(
        this,
        'categoryMetrics',
        'currentCategoryFilter'
      );

      return categoryMetrics.map(category => ({
        ...category,
        isFaded: !!currentCategoryFilter && category.name !== currentCategoryFilter
      }));
    }
  );

  /**
   * Modified severityMetrics to add properties that will help us render our actual charts
   * @type {ComputedProperty<Array<IChartDatum>>}
   */
  renderedSeverity: ComputedProperty<Array<IChartDatum>> = computed(
    'severityMetrics',
    'currentSeverityFilter',
    function(this: DatasetHealthContainer): Array<IChartDatum> {
      const { severityMetrics, currentSeverityFilter } = getProperties(
        this,
        'severityMetrics',
        'currentSeverityFilter'
      );

      return severityMetrics.map(severity => ({
        ...severity,
        isFaded: !!currentSeverityFilter && severity.name !== currentSeverityFilter,
        customColorClass: `severity-chart__bar--${severity.name.toLowerCase()}`
      }));
    }
  );

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
  getContainerDataTask = task(function*(this: DatasetHealthContainer): IterableIterator<Promise<IDatasetHealth>> {
    const { health } = yield readDatasetHealthByUrn(get(this, 'urn'));
    // Pretend like we're getting data from somehwere
    const healthData = {
      categories: healthCategories,
      severity: healthSeverity,
      detail: healthDetail
    };

    setProperties(this, {
      categoryMetrics: healthData.categories,
      severityMetrics: healthData.severity,
      tableData: healthData.detail
    });

    return health; // Do something with health information
  });

  /**
   * Triggered when the user clicks on one of the bars in the summary charts child component, will trigger
   * a filter for whatever bar they select, unless it already is one in which case we will remove the filter
   * @param this - Explicit this declaration for typescript
   * @param filterType - Whether we are filtering by category or severity
   * @param filterDatum - Passed in to the action by the child component, contains the tag to be filtered for
   */
  onFilterSelect(this: DatasetHealthContainer, filterType: string, filterDatum: IChartDatum): void {
    const { currentCategoryFilter, currentSeverityFilter } = getProperties(
      this,
      'currentCategoryFilter',
      'currentSeverityFilter'
    );
    const newFilterName = filterDatum.name || filterDatum.value.toString();

    setProperties(this, {
      currentCategoryFilter: filterType === 'category' && newFilterName !== currentCategoryFilter ? newFilterName : '',
      currentSeverityFilter: filterType === 'severity' && newFilterName !== currentSeverityFilter ? newFilterName : ''
    });
  }
}
