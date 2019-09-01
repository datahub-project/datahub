import Component from '@ember/component';
import { computed } from '@ember/object';
import { IChartDatum } from 'wherehows-web/typings/app/visualization/charts';
import { set } from '@ember/object';
import { noop } from 'wherehows-web/utils/helpers/functions';
import { classNames, tagName } from '@ember-decorators/component';
import { arrayReduce } from 'wherehows-web/utils/array';
import $ from 'jquery';

interface IBarSeriesDatum extends IChartDatum {
  yOffset: number;
  barLength: number;
  labelOffset: number;
}

/**
 * This custom component exists outside of highcharts as the library does not provide the amount
 * of capabilities we need to match up with our design vision for horizontal bar charts. As such,
 * there are similarities between this component and a highcharts component but it has been
 * tailor-fit to our needs
 *
 * Bar Chart Usage
 * {{visualization/charts/horizontal-bar-chart
 *   series=[ { name: string, value: number, otherKey: otherValue } ]
 *   title="string"
 *   labelTagProperty="optionStringOverridesDefault"
 *   labelAppendTag="optionalStringAppendsEachTag"
 *   labelAppendValue="optionalStringSuchAs%"}}
 */
@tagName('figure')
@classNames('viz-chart', 'viz-bar-chart', 'single-series')
export default class HorizontalBarChart extends Component {
  /**
   * Represents the series of data needed to power our chart. Format is
   * [ { name: string, value: number } ].
   * Since this chart is only meant to handle a single series of data where each bar is connected
   * to one value with one label, we don't have to worry about the idea of an "x axis * y axis"
   * @type {Array<IChartDatum>}
   */
  series: Array<IChartDatum>;

  /**
   * Helps to set the size of the svg element rendered by the component
   * @type {number}
   */
  size: number = 0;

  /**
   * Property in the series datum to use as the tag for each value in the bar legend. Note, each
   * legend item will appear as VALUE | TAG
   * @type {string}
   * @default 'name'
   */
  labelTagProperty: string = 'name';

  /**
   * Any string we want to append to each tag in the label, such as a unit.
   * @type {string}
   */
  labelAppendTag: string = '';

  /**
   * Any string that we want to append to each value in the label, such as %. Doing so would
   * append every value, such as 60, in the label with % and appear as 60%
   * @type {string}
   */
  labelAppendValue: string = '';

  /**
   * Constant properties to be used in calculations for the size of the svg elements drawn
   * @type {number}
   */
  BAR_HEIGHT = 16;
  BAR_MARGIN_BOTTOM = 8;
  LABEL_HEIGHT = 15;
  LABEL_MARGIN_BOTTOM = 16;

  /**
   * Overall width of our chart. If we have a size, that means that the component and available space
   * has been measured.
   * @type {number}
   */
  @computed('size')
  get width(): number {
    return this.size ? $(this.element).width() || 0 : 0;
  }

  /**
   * Overall height of our chart, calculated based on the amount of items we have in our series
   * @type {number}
   */
  @computed('categories')
  get height(): number {
    return (this.series || []).length * this.heightModifier();
  }

  /**
   * Calculates information needed for the svg element to properly render each bar of our graph using the
   * correct dimensions relative to the data it's receiving
   * @type {ComputedProperty<IBarSeriesDatum[]}
   */
  @computed('series', 'size')
  get seriesData(): Array<IBarSeriesDatum> {
    return (this.series || []).map(this.bar.bind(this));
  }

  /**
   * Sets our highest value for the chart's Y axis, based on the highest value inside the series
   * @type {number}
   */
  @computed('series')
  get maxY(): number {
    return arrayReduce((maxY: number, { value }: IChartDatum) => (value > maxY ? value : maxY), Number.MIN_VALUE)(
      this.series || ([] as Array<IChartDatum>)
    );
  }

  /**
   * Returns a "modifier" that is the height of a single bar and label in the chart, and can be multiplied
   * by the number of rows in the chart to get the total chart height
   */
  heightModifier = (): number =>
    this.BAR_HEIGHT + this.BAR_MARGIN_BOTTOM + this.LABEL_HEIGHT + this.LABEL_MARGIN_BOTTOM;

  /**
   * Used as a predicate function in the mapping function for the series array to be mapped into the
   * seriesData array, this function adds values to each chart datum object so that the svg template
   * can render each bar with the correct dimensions and position
   * @param this - explicit this keyword declaration for typescript
   * @param data - single datum object in our series
   * @param index - current index in the series array
   */
  bar(this: HorizontalBarChart, data: IChartDatum, index: number): IBarSeriesDatum {
    const { maxY, width, BAR_HEIGHT, BAR_MARGIN_BOTTOM, LABEL_HEIGHT, heightModifier } = this;
    const yOffset = 1 + index * heightModifier();

    return {
      ...data,
      yOffset,
      barLength: Math.max(1, Math.floor((data.value / maxY) * width)),
      labelOffset: yOffset + BAR_HEIGHT + BAR_MARGIN_BOTTOM + LABEL_HEIGHT
    };
  }

  /**
   * Expected to be optionally passed in from the containing component, this function handles the action to
   * be taken if a user selects an individual bar from the chart.
   * @param {string} name - the "category" or "tag" that goes with each legend label
   * @param {number} value - the value associated with each series datum
   */
  onBarSelect: (name: string, value: number) => void = noop;

  /**
   * Once we have inserted our html element, we can determine the width (size) of our chart
   */
  didInsertElement(): void {
    super.didInsertElement();
    set(this, 'size', $(this.element).width() || 0);
  }
}
