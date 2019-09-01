/**
 * Expected basic chart data object for a single item in a chart series.
 */
export interface IChartDatum {
  name: string;
  value: number;
  isFaded?: boolean;
  customColorClass?: string;
}

/**
 * Expected parameters for a high charts configuration object. This will probably be expanded as
 * we deal with more charts and use cases but the starting generic point is based on gauges
 */
export interface IHighChartsConfig {
  chart: { type: string; backgroundColor?: string };
  title?: string;
  pane?: {
    center?: Array<string>;
    size?: string;
    startAngle?: number;
    endAngle?: number;
    background?: {
      backgroundColor?: string;
      innerRadius?: string;
      outerRadius?: string;
      shape?: string;
      borderColor?: string;
    };
  };
  tooltip?: { enabled?: boolean };
  plotOptions?: any;
}

/**
 * Expected parameters for a high charts solid gauge configuration object. This may be expanded as
 * we deal with more gauge use cases
 */
export interface IHighChartsGaugeConfig extends IHighChartsConfig {
  yAxis: {
    min: number;
    max: number;
    stops: Array<Array<string | number>>;
    minorTickInterval?: any;
    tickPixelInterval: number;
    tickWidth: number;
    gridLineWidth: number;
    gridLineColor: string;
    labels?: { enabled?: boolean };
    title?: { enabled?: boolean };
  };
  credits?: { enabled?: boolean };
  plotOptions: {
    solidgauge: {
      innerRadius?: string;
      dataLabels?: { enabled?: boolean };
    };
  };
}

export interface IHighChartsDataConfig {
  name?: string;
  // May need to be refined as we develop new kinds of charts
  data: Array<number>;
  dataLabels?: {
    format?: string;
  };
}
