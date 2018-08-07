import Component from '@ember/component';
import { computed, setProperties, getProperties, get } from '@ember/object';
import { IHighChartsGaugeConfig, IHighChartsDataConfig } from 'wherehows-web/typings/app/visualization/charts';

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
export default class VisualizationChartsScoreGauge extends Component {
  /**
   * Sets the classes for the rendered html element for the component
   * @type {Array<string>}
   */
  classNames = ['score-gauge'];

  /**
   * Displays a passed in chart title.
   * @default ''
   */
  title: string;

  /**
   * Fetched score data in order to render onto the graph
   * @default NaN
   */
  score: number;

  /**
   * Represents the maximum value a score can be. Helps us to calculate a percentage score
   * @default 100
   */
  maxScore: number;

  /**
   * Format option to determine how to display our score in the legend label
   * @type {ScoreDisplay}
   * @default ScoreDisplay.percentage
   */
  scoreDisplay: ScoreDisplay;

  /**
   * Gives a simple access to the chart state for other computed values to use
   * @type {ComputedProperty<string>}
   */
  chartState = computed('score', function(): string {
    const { score, maxScore } = getProperties(this, 'score', 'maxScore');
    const percentageScore = score / maxScore;

    if (percentageScore <= 0.25) {
      return ScoreState.critical;
    }

    if (percentageScore <= 0.5) {
      return ScoreState.warning;
    }

    return ScoreState.good;
  });

  /**
   * Computes the class to properly color the legend value between the different states
   * @type {ComputedProperty<string>}
   */
  labelValueClass = computed('chartState', function(): string {
    return `score-gauge__legend-value--${get(this, 'chartState')}`;
  });

  scoreAsPercentage = computed('score', function(): number {
    const { score, maxScore } = getProperties(this, 'score', 'maxScore');

    return Math.round((score / maxScore) * 100);
  });

  /**
   * Creates a fresh configuration for our gauge chart every time we init a new instance of this
   * component class
   * @type {ComputedProperty<IHighChartsGaugeConfig>}
   */
  chartOptions = computed(function(): IHighChartsGaugeConfig {
    return {
      chart: { type: 'solidgauge', backgroundColor: 'transparent' },
      title: '',
      pane: {
        center: ['50%', '50%'],
        size: '100%',
        startAngle: 0,
        endAngle: 360,
        background: {
          backgroundColor: '#ddd',
          innerRadius: '90%',
          outerRadius: '100%',
          shape: 'arc',
          borderColor: 'transparent'
        }
      },
      tooltip: {
        enabled: false
      },
      yAxis: {
        min: 0,
        max: 100,
        stops: [
          [0.25, '#ff2c33'], // get-color(red5)
          [0.5, '#e55800'], // get-color(orange5)
          [0.75, '#469a1f'] // get-color(green5)
        ],
        minorTickInterval: null,
        tickPixelInterval: 400,
        tickWidth: 0,
        gridLineWidth: 0,
        gridLineColor: 'transparent',
        labels: {
          enabled: false
        },
        title: {
          enabled: false
        }
      },
      credits: {
        enabled: false
      },
      plotOptions: {
        solidgauge: {
          innerRadius: '90%',
          dataLabels: {
            enabled: false
          }
        }
      }
    };
  });

  /**
   * Creates a fresh copy of the data object in the format expected by the highcharts "content" reader.
   */
  chartData = computed(function(): Array<IHighChartsDataConfig> {
    return [{ name: 'score', data: [0] }];
  });

  constructor() {
    super(...arguments);

    const { chartOptions, chartData } = getProperties(this, 'chartOptions', 'chartData');
    const maxScore = typeof this.maxScore === 'number' ? this.maxScore : 100;
    const score = this.score || NaN;
    // Adds our information to the highcharts formatted configurations so that they can be read in the chart
    chartOptions.yAxis.max = maxScore;
    chartData[0].data = [score];

    setProperties(this, {
      score,
      maxScore,
      title: this.title || '',
      scoreDisplay: this.scoreDisplay || ScoreDisplay.percentage
    });
  }
}
