import React from 'react';

import styled from 'styled-components';
import { Typography } from 'antd';
import {
    Assertion,
    AssertionRunEventsResult,
    AssertionRunStatus,
} from '../../../../../../../../../../../types.generated';
import { ValuesOverTimeAssertionResultChart } from './charts/ValuesOverTimeAssertionResultChart';
import { AssertionChartType, AssertionResultChartData, TimeRange } from './charts/types';
import { getAssertionResultChartData } from './transformers';
import { getBestChartTypeForAssertion } from './charts/utils';
import { StatusOverTimeAssertionResultChart } from './charts/StatusOverTimeAssertionResultChart';
import { ANTD_GRAY } from '../../../../../../../../constants';
import { getTimeRangeDisplay } from './utils';

const VIZ_CONTAINER_HEIGHT = 200;
const VIZ_CONTAINER_TITLE_HEIGHT = 36;

const VisualizationContainer = styled.div`
    background-color: ${ANTD_GRAY[2]};
    border-radius: 4px;
    height: ${VIZ_CONTAINER_HEIGHT}px;
    display: flex;
    flex-direction: column;
    align-items: center;
    padding: 8px;
`;

const VizHeader = styled.div`
    width: 100%;
    height: ${VIZ_CONTAINER_TITLE_HEIGHT}px;
    display: flex;
    align-items: center;
    justify-content: left;
`;

const VizHeaderTitle = styled(Typography.Text)`
    margin: 12px;
    margin-bottom: 20px;
    margin-top: 4px;
    color: ${ANTD_GRAY[9]};
    font-size: 12px;
`;

type Props = {
    assertion: Assertion;
    timeRange: TimeRange;
    results?: AssertionRunEventsResult | null;
    parentDimensions: { width: number }
};

export const AssertionResultsTimelineViz = ({ assertion, results, timeRange, parentDimensions }: Props) => {
    // Run event data
    const completedRuns =
        results?.runEvents.filter((runEvent) => runEvent.status === AssertionRunStatus.Complete) || [];

    const assertionResultChartData: AssertionResultChartData = getAssertionResultChartData(
        assertion,
        completedRuns,
    )

    // Date range
    const dateRange = getTimeRangeDisplay(timeRange);

    // render
    const chartDimensions = {
        height: VIZ_CONTAINER_HEIGHT - VIZ_CONTAINER_TITLE_HEIGHT - 8, // margin below (flex-start)
        width: parentDimensions.width - 8, // margin on the sides (we have align-items=center)
    }

    return <VisualizationContainer>
        <VizHeader>
            <VizHeaderTitle strong>{assertionResultChartData.yAxisLabel ? `${assertionResultChartData.yAxisLabel} over time` : dateRange}</VizHeaderTitle>
        </VizHeader>
        {getBestChartTypeForAssertion(assertion.info) === AssertionChartType.ValuesOverTime
            ? <ValuesOverTimeAssertionResultChart
                chartDimensions={chartDimensions}
                data={assertionResultChartData}
                timeRange={timeRange}
            />
            : <StatusOverTimeAssertionResultChart
                chartDimensions={chartDimensions}
                data={assertionResultChartData}
                timeRange={timeRange}
            />}
    </VisualizationContainer>
};
