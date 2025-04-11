import React from 'react';

import styled from 'styled-components';
import { Typography } from 'antd';
import {
    Assertion,
    AssertionRunEventsResult,
    AssertionRunStatus,
} from '../../../../../../../../../../../types.generated';
import { AssertionResultChartData, TimeRange } from './charts/types';
import { getAssertionResultChartData } from './transformers';
import { StatusOverTimeAssertionResultChart } from './charts/StatusOverTimeAssertionResultChart';
import { ANTD_GRAY } from '../../../../../../../../constants';
import { getTimeRangeDisplay } from './utils';

const VIZ_CONTAINER_TITLE_HEIGHT = 36;

const getVisualizationContainer = (height: number) => styled.div`
    border-radius: 4px;
    height: ${height}px;
    display: flex;
    flex-direction: column;
    align-items: center;
`;

const VizHeader = styled.div`
    width: 100%;
    height: ${VIZ_CONTAINER_TITLE_HEIGHT}px;
    display: flex;
    align-items: center;
    justify-content: left;
`;

const VizHeaderTitle = styled(Typography.Text)`
    margin-bottom: 20px;
    margin-top: 4px;
    color: ${ANTD_GRAY[9]};
    font-size: 16px;
    font-weight: 600;
`;

type Props = {
    assertion: Assertion;
    timeRange: TimeRange;
    results?: AssertionRunEventsResult | null;
    isInitializing: boolean;
    parentDimensions: { width: number; height: number };
};

export const AssertionResultsTimelineViz = ({
    assertion,
    results,
    timeRange,
    parentDimensions,
    isInitializing,
}: Props) => {
    // Run event data
    const completedRuns =
        results?.runEvents?.filter((runEvent) => runEvent.status === AssertionRunStatus.Complete) || [];

    const assertionResultChartData: AssertionResultChartData = getAssertionResultChartData(assertion, completedRuns);

    // render
    const chartDimensions = {
        height: parentDimensions.height - VIZ_CONTAINER_TITLE_HEIGHT - 8, // margin below (flex-start)
        width: parentDimensions.width - 8, // margin on the sides (we have align-items=center)
    };

    const renderChartTitle = (title?: string) => (
        <VizHeader>
            <VizHeaderTitle strong>{title || getTimeRangeDisplay(timeRange)}</VizHeaderTitle>
        </VizHeader>
    );

    const renderChart = (): JSX.Element | undefined => {
        return (
            <StatusOverTimeAssertionResultChart
                chartDimensions={chartDimensions}
                data={assertionResultChartData}
                timeRange={timeRange}
                renderHeader={renderChartTitle}
            />
        );
    };

    const VisualizationContainer = getVisualizationContainer(parentDimensions.height);
    return <VisualizationContainer style={{ opacity: isInitializing ? 0 : 1 }}>{renderChart()}</VisualizationContainer>;
};
