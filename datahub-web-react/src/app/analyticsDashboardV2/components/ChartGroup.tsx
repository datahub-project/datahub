import { PageTitle } from '@components';
import { Col, Row } from 'antd';
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

const ChartCol = styled(Col)`
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
            <Row gutter={[16, 16]}>
                {chartGroup.charts.map((chart) => (
                    <ChartCol key={chart.title} sm={24} md={24} lg={8} xl={8}>
                        <AnalyticsChart
                            chartData={chart}
                            testId={`analytics-${chart.title?.toLowerCase().replace(/\s+/g, '-')}`}
                        />
                    </ChartCol>
                ))}
            </Row>
        </Container>
    );
};
