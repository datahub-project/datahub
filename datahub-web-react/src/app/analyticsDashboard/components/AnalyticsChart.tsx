import React from 'react';
import { Typography } from 'antd';

import { AnalyticsChart as AnalyticsChartType } from '../../../types.generated';
import { TimeSeriesChart } from './TimeSeriesChart';
import { BarChart } from './BarChart';
import { TableChart } from './TableChart';
import { ChartCard } from './ChartCard';
import { ChartContainer } from './ChartContainer';

type Props = {
    chartData: AnalyticsChartType;
    width: number;
    height: number;
};

export const AnalyticsChart = ({ chartData, width, height }: Props) => {
    let chartSection: React.ReactNode = null;
    const isTable = chartData.__typename === 'TableChart';

    switch (chartData.__typename) {
        case 'TimeSeriesChart':
            chartSection = <TimeSeriesChart insertBlankPoints chartData={chartData} width={width} height={height} />;
            break;
        case 'BarChart':
            chartSection = <BarChart chartData={chartData} width={width} height={height} />;
            break;
        case 'TableChart':
            chartSection = <TableChart chartData={chartData} />;
            break;
        default:
            break;
    }

    return (
        <ChartCard $shouldScroll={isTable}>
            <ChartContainer>
                <div>
                    <Typography.Title level={5}>{chartData.title}</Typography.Title>
                </div>
                {chartSection}
            </ChartContainer>
        </ChartCard>
    );
};
