import { Typography } from 'antd';
import React from 'react';

import { BarChart } from '@app/analyticsDashboard/components/BarChart';
import { ChartCard } from '@app/analyticsDashboard/components/ChartCard';
import { ChartContainer } from '@app/analyticsDashboard/components/ChartContainer';
import { TableChart } from '@app/analyticsDashboard/components/TableChart';
import { TimeSeriesChart } from '@app/analyticsDashboard/components/TimeSeriesChart';

import { AnalyticsChart as AnalyticsChartType } from '@types';

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
