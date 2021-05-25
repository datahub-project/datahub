import React from 'react';
import { Card, Typography } from 'antd';
import styled from 'styled-components';

import { AnalyticsChart as AnalyticsChartType } from '../../../types.generated';
import { TimeSeriesChart } from './TimeSeriesChart';
import { BarChart } from './BarChart';
import { TableChart } from './TableChart';

type Props = {
    chartData: AnalyticsChartType;
    width: number;
    height: number;
};

const Container = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    flex-direction: column;
`;

const ChartCard = styled(Card)<{ shouldScroll: boolean }>`
    margin: 16px;
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
    height: 440px;
    overflow-y: ${(props) => (props.shouldScroll ? 'scroll' : 'hidden')};
`;

export const AnalyticsChart = ({ chartData, width, height }: Props) => {
    let chartSection: React.ReactNode = null;
    const isTable = chartData.__typename === 'TableChart';

    switch (chartData.__typename) {
        case 'TimeSeriesChart':
            chartSection = <TimeSeriesChart chartData={chartData} width={width} height={height} />;
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
        <ChartCard shouldScroll={isTable}>
            <Container>
                <div>
                    <Typography.Title level={5}>{chartData.title}</Typography.Title>
                </div>
                {chartSection}
            </Container>
        </ChartCard>
    );
};
