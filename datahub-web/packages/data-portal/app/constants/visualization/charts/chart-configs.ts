import { IHighChartsGaugeConfig, IHighChartsDataConfig } from 'wherehows-web/typings/app/visualization/charts';

export function getBaseChartDataConfig(name: string): Array<IHighChartsDataConfig> {
  return [{ name, data: [0] }];
}

export function getBaseGaugeConfig(): IHighChartsGaugeConfig {
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
}
