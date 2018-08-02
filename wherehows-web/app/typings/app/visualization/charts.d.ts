/**
 * Expected basic chart data object for a single item in a chart series.
 */
export interface IChartDatum {
  name: string;
  value: number;
  isFaded?: boolean;
  customColorClass?: string;
}
