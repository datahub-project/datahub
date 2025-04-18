import React from 'react';
import styled from 'styled-components';
import { Col, Divider, Row, Typography } from 'antd';
import { AnalyticsChartGroup } from '../../../types.generated';
import { AnalyticsChart } from './AnalyticsChart';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    padding-top: 20px;
    padding-left: 12px;
    padding-right: 12px;
`;

const TitleContainer = styled.div`
    margin-left: 16px;
    padding-left: 8px;
    padding-right: 8px;
    padding-top: 8px;
`;

const GroupTitle = styled(Typography.Title)`
    &&& {
        margin-bottom: -12px;
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
                    <GroupTitle level={3}>{chartGroup.title}</GroupTitle>
                    <Divider />
                </TitleContainer>
            )}
            <Row>
                {chartGroup.charts.map((chart) => (
                    <Col sm={24} md={24} lg={8} xl={8}>
                        <AnalyticsChart chartData={chart} width={300} height={300} />
                    </Col>
                ))}
            </Row>
        </Container>
    );
};
