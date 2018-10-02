import Component from '@ember/component';
import { computed, setProperties, getProperties, get } from '@ember/object';
import { task } from 'ember-concurrency';
import ComputedProperty from '@ember/object/computed';
import { IChartDatum } from 'wherehows-web/typings/app/visualization/charts';
import { IHealthScore, IDatasetHealth } from 'wherehows-web/typings/api/datasets/health';
import { readDatasetHealthByUrn, getCategory } from 'wherehows-web/utils/api/datasets/health';
import { Tabs } from 'wherehows-web/constants/datasets/shared';
import { equal } from '@ember-decorators/object/computed';
import { IObject } from 'wherehows-web/typings/generic';

/**
 * Used for the dataset health tab, represents the fieldnames for the health score table
 */
export enum HealthDataFields {
  category = 'Category',
  description = 'Description',
  score = 'Score',
  severity = 'Priority'
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
   * Passed in from the higher level component, we use this property in order to determine whether the dataset health
   * tab is the currently selected tab
   * @type {Tabs}
   */
  tabSelected: Tabs;

  /**
   * Calculated from the currently selected tab to determine whether this container is the currently selected tab.
   * Note: Highcharts calculates size and other chart details upon initial render and doesn't do a good job of handling
   * rerenders. Because of this we want those calculations to take place when dataset health is the currently active tab,
   * otherwise we will insert elements off screen and size will default to 0 and we lose our charts
   * @type {ComputedProperty<boolean>}
   */
  @equal('tabSelected', Tabs.Health)
  isActiveTab: boolean;

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
    const health: IDatasetHealth = yield readDatasetHealthByUrn(this.urn);

    const details = health.validations || [];
    const total = details.length;
    const categories: IObject<number> = {};
    const severities: IObject<number> = {};

    // Go through the details and find the COUNT of severity and category groupings
    const tableData: Array<IHealthScore> = details.map(detail => {
      const category = getCategory(detail.validator);
      const severity = detail.tier || 'none';
      categories[category] = (categories[category] || 0) + 1;
      severities[severity] = (severities[severity] || 0) + 1;

      return { category, severity, description: detail.description, score: detail.score * 100 };
    });

    const categoryMetrics: Array<IChartDatum> = Object.keys(categories).map(category => ({
      name: category,
      value: Math.round((categories[category] / total) * 100)
    }));

    const severityMetrics: Array<IChartDatum> = Object.keys(severities).map(severity => ({
      name: severity,
      value: Math.round((severities[severity] / total) * 100)
    }));

    setProperties(this, {
      categoryMetrics,
      severityMetrics,
      tableData
    });
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
