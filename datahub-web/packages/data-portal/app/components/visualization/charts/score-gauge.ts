import Component from '@ember/component';
import { setProperties } from '@ember/object';
import { computed } from '@ember/object';
import { IHighChartsGaugeConfig, IHighChartsDataConfig } from 'wherehows-web/typings/app/visualization/charts';
import { getBaseGaugeConfig, getBaseChartDataConfig } from 'wherehows-web/constants/visualization/charts/chart-configs';
import { classNames } from '@ember-decorators/component';

/**
 * Whether the score is in the good (51%+), warning (26-50%) or critical (0-25% range)
 */
export enum ScoreState {
  good = 'good',
  warning = 'warning',
  critical = 'critical'
}

/**
 * How we want to display our score. If our score is 166 and max score is 200, percentage will display as
 * 83%, outOf will display as 166 / 200, and number will display as 166
 */
export enum ScoreDisplay {
  percentage = 'percent',
  outOf = 'outOf',
  number = 'number'
}

/**
 * This score gauge component was originally developed to handle showing metadata health score gauges for a
 * particular dataset. It appears as a basic circle gauge that changes colors depending on how far along we
 * are in terms of score "percentage" and includes a simple legend and value display. There are currently
 * no user interactions with this component.
 *
 * @example
 * {{visualization/charts/score-gauge
 *   title="string"
 *   score=numberValue
 *   maxScore=optionalNumberValue
 *   scoreDisplay="percent" // Optional, defaults to "percent" but values can also be "outOf" and "number",
 *                          // see details in class definition
 * }}
 */
@classNames('score-gauge')
export default class VisualizationChartsScoreGauge extends Component {
  /**
   * Displays a passed in chart title.
   * @type {number}
   * @default ''
   */
  title: string = '';

  /**
   * Fetched score data in order to render onto the graph
   * @type {number}
   * @default 0
   */
  score: number = 0;

  /**
   * Represents the maximum value a score can be. Helps us to calculate a percentage score
   * @type {number}
   * @default 100
   */
  maxScore: number = 100;

  /**
   * Format option to determine how to display our score in the legend label
   * @type {ScoreDisplay}
   * @default ScoreDisplay.percentage
   */
  scoreDisplay: ScoreDisplay = ScoreDisplay.percentage;

  /**
   * Gives a simple access to the chart state for other computed values to use
   * @type {string}
   */
  @computed('score')
  get chartState(): ScoreState {
    const { scoreAsPercentage } = this;

    if (scoreAsPercentage <= 25) {
      return ScoreState.critical;
    }

    if (scoreAsPercentage <= 50) {
      return ScoreState.warning;
    }

    return ScoreState.good;
  }

  /**
   * Computes the class to properly color the legend value between the different states
   * @type {string}
   */
  @computed('chartState')
  get labelValueClass(): string {
    return `score-gauge__legend-value--${this.chartState}`;
  }

  /**
   * Computes the score as a percentage in order to determine the score state property as well as use
   * in the template to display the numerical score if we choose to display as a percentage
   * @type {number}
   */
  @computed('score')
  get scoreAsPercentage(): number {
    const { score, maxScore } = this;

    return Math.round((score / maxScore) * 100);
  }

  /**
   * Creates a fresh configuration for our gauge chart every time we init a new instance of this
   * component class
   * @type {IHighChartsGaugeConfig}
   */
  chartOptions: IHighChartsGaugeConfig;

  /**
   * Creates a fresh copy of the data object in the format expected by the highcharts "content" reader.
   */
  chartData: Array<IHighChartsDataConfig>;

  /**
   * Performs update functions for the charts upon initial and subsequent renders
   */
  updateChart(): void {
    const chartOptions = getBaseGaugeConfig();
    const chartData = getBaseChartDataConfig('score');
    const score = this.score || 0;

    // Adds our information to the highcharts formatted configurations so that they can be read in the chart
    chartOptions.yAxis.max = this.maxScore;
    chartData[0].data = [score];

    setProperties(this, {
      chartOptions,
      chartData
    });
  }

  /**
   * Allows us to rerender the graph when the score gets updated. Useful for when API calls haven't been
   * resolved yet at initial render
   */
  didUpdateAttrs() {
    super.didUpdateAttrs();
    this.updateChart();
  }

  init() {
    super.init();
    this.updateChart();
  }
}
