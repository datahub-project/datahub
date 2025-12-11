/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Col, Divider, Row, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { AnalyticsChart } from '@app/analyticsDashboard/components/AnalyticsChart';

import { AnalyticsChartGroup } from '@types';

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
