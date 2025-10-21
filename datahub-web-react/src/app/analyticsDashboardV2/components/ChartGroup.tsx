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
    padding: 24px 16px;
`;

const TitleContainer = styled.div`
    margin-bottom: 8px;
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
};

export const ChartGroup = ({ chartGroup }: Props) => {
    return (
        <Container>
            {chartGroup.title?.length > 0 && (
                <TitleContainer>
                    <PageTitle title={chartGroup.title} variant="sectionHeader" />
                </TitleContainer>
            )}
            <Row gutter={[16, 16]}>
                {chartGroup.charts.map((chart) => (
                    <ChartCol key={chart.title} sm={24} md={24} lg={8} xl={8}>
                        <AnalyticsChart chartData={chart} />
                    </ChartCol>
                ))}
            </Row>
        </Container>
    );
};
