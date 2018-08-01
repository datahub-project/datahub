import Component from '@ember/component';
import { IChartDatum } from 'wherehows-web/typings/app/visualization/charts';
import { set } from '@ember/object';
import { noop } from 'wherehows-web/utils/helpers/functions';

export default class DatasetsHealthMetricsCharts extends Component {
  /**
   * Sets the classes for the rendered html element for the compoennt
   * @type {Array<string>}
   */
  classNames = ['dataset-health__metrics-charts'];

  /**
   * Pass through function meant to come from the dataset-health container that handles the selection of
   * a filter, triggered when the user clicks on one of the bars of the bar chart.
   * @param {IChartDatum} datum - the actual chart datum object so we know what was clicked
   */
  onFilterSelect: (datum: IChartDatum) => void;

  constructor() {
    super(...arguments);

    set(this, 'onFilterSelect', this.onFilterSelect || noop);
  }
}
