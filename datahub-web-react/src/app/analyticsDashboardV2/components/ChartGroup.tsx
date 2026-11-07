import { PageTitle } from '@components';
import React from 'react';
import styled from 'styled-components';

import { AnalyticsChart } from '@app/analyticsDashboardV2/components/AnalyticsChart';

import { AnalyticsChartGroup } from '@types';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
    margin-bottom: 24px;
`;

// Matches the previous antd grid: one chart per row below the lg breakpoint (992px), three per row above it.
const ChartGrid = styled.div`
    display: grid;
    grid-template-columns: minmax(0, 1fr);
    gap: 16px;

    @media (min-width: 992px) {
        grid-template-columns: repeat(3, minmax(0, 1fr));
    }
`;

const ChartCol = styled.div`
    display: flex;
    flex-direction: column;

    > * {
        flex: 1;
        display: flex;
        flex-direction: column;
    }
`;

type Props = {
    chartGroup: AnalyticsChartGroup;
    testId?: string;
};

export const ChartGroup = ({ chartGroup, testId }: Props) => {
    return (
        <Container data-testid={testId}>
            {chartGroup.title?.length > 0 && <PageTitle title={chartGroup.title} variant="sectionHeader" />}
            <ChartGrid>
                {chartGroup.charts.map((chart) => (
                    <ChartCol key={chart.title}>
                        <AnalyticsChart
                            chartData={chart}
                            testId={`analytics-${chart.title?.toLowerCase().replace(/\s+/g, '-')}`}
                        />
                    </ChartCol>
                ))}
            </ChartGrid>
        </Container>
    );
};
