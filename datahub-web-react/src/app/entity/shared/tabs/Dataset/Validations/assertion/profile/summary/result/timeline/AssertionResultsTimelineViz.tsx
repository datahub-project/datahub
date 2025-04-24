import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { FreshnessResultChart } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/charts/FreshnessResultChart';
import { StatusOverTimeAssertionResultChart } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/charts/StatusOverTimeAssertionResultChart';
import { ValuesOverTimeAssertionResultChart } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/charts/ValuesOverTimeAssertionResultChart';
import {
    AssertionChartType,
    AssertionResultChartData,
    TimeRange,
} from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/charts/types';
import { getBestChartTypeForAssertion } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/charts/utils';
import { getAssertionResultChartData } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/transformers';
import { getTimeRangeDisplay } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/utils';

import { Assertion, AssertionRunEventsResult, AssertionRunStatus } from '@types';

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
        results?.runEvents.filter((runEvent) => runEvent.status === AssertionRunStatus.Complete) || [];

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
        switch (getBestChartTypeForAssertion(assertion.info)) {
            case AssertionChartType.ValuesOverTime:
                return (
                    <ValuesOverTimeAssertionResultChart
                        chartDimensions={chartDimensions}
                        data={assertionResultChartData}
                        timeRange={timeRange}
                        renderHeader={renderChartTitle}
                    />
                );
            case AssertionChartType.Freshness:
                return (
                    <FreshnessResultChart
                        chartDimensions={chartDimensions}
                        data={assertionResultChartData}
                        timeRange={timeRange}
                        renderHeader={renderChartTitle}
                    />
                );
            default:
                return (
                    <StatusOverTimeAssertionResultChart
                        chartDimensions={chartDimensions}
                        data={assertionResultChartData}
                        timeRange={timeRange}
                        renderHeader={renderChartTitle}
                    />
                );
        }
    };

    const VisualizationContainer = getVisualizationContainer(parentDimensions.height);
    return <VisualizationContainer style={{ opacity: isInitializing ? 0 : 1 }}>{renderChart()}</VisualizationContainer>;
};
