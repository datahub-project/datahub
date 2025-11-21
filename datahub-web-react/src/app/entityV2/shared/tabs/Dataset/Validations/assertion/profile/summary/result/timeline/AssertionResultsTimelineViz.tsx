import React, { useState } from 'react';
import styled from 'styled-components';

import { FreshnessResultChart } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/charts/FreshnessResultChart';
import { StatusOverTimeAssertionResultChart } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/charts/StatusOverTimeAssertionResultChart';
import { ValuesOverTimeAssertionResultChart } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/charts/ValuesOverTimeAssertionResultChart';
import {
    AssertionChartType,
    AssertionResultChartData,
    TimeRange,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/charts/types';
import { getBestChartTypeForAssertion } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/charts/utils';
import { getAssertionResultChartData } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/transformers';
import { TuneSmartAssertionModal } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/TuneSmartAssertionModal';

import { Assertion, AssertionRunEventsResult, AssertionRunStatus, Maybe, Monitor } from '@types';

const VIZ_CONTAINER_TITLE_HEIGHT = 36;

const VizContainer = styled.div<{ height: number }>`
    border-radius: 4px;
    height: ${({ height }) => height}px;
    display: flex;
    flex-direction: column;
    align-items: center;
`;

type Props = {
    assertion: Assertion;
    monitor?: Maybe<Monitor>;
    timeRange: TimeRange;
    results?: AssertionRunEventsResult | null;
    isInitializing: boolean;
    parentDimensions: { width: number; height: number };
    refreshData?: () => Promise<unknown>;
    openAssertionNote?: () => void;
    onTimeRangeChange?: (startTimeMs: number, endTimeMs: number) => void;
};

export const AssertionResultsTimelineViz = ({
    assertion,
    monitor,
    results,
    timeRange,
    parentDimensions,
    isInitializing,
    refreshData,
    openAssertionNote,
    onTimeRangeChange,
}: Props) => {
    const [isTunePredictionsModalOpen, setIsTunePredictionsModalOpen] = useState(false);

    // Run event data
    const completedRuns =
        results?.runEvents?.filter((runEvent) => runEvent.status === AssertionRunStatus.Complete) || [];

    const assertionResultChartData: AssertionResultChartData = getAssertionResultChartData(
        assertion,
        completedRuns,
        monitor,
    );

    const exclusionWindows = monitor?.info?.assertionMonitor?.settings?.inferenceSettings?.exclusionWindows ?? [];

    // render
    const chartDimensions = {
        height: parentDimensions.height - VIZ_CONTAINER_TITLE_HEIGHT - 28, // margin below (flex-start)
        width: parentDimensions.width - 8, // margin on the sides (we have align-items=center)
    };

    const bestChartType = getBestChartTypeForAssertion(assertion.info);
    const handleOpenTunePredictionsModal = () => {
        setIsTunePredictionsModalOpen(true);
    };

    const renderChart = (): JSX.Element | undefined => {
        switch (bestChartType) {
            case AssertionChartType.ValuesOverTime:
                return (
                    <ValuesOverTimeAssertionResultChart
                        chartDimensions={chartDimensions}
                        data={assertionResultChartData}
                        exclusionWindows={exclusionWindows}
                        timeRange={timeRange}
                        refreshData={refreshData}
                        openAssertionNote={openAssertionNote}
                        onTimeRangeChange={onTimeRangeChange}
                        onOpenTunePredictionsModal={handleOpenTunePredictionsModal}
                    />
                );
            case AssertionChartType.Freshness:
                return (
                    <FreshnessResultChart
                        chartDimensions={chartDimensions}
                        data={assertionResultChartData}
                        exclusionWindows={exclusionWindows}
                        timeRange={timeRange}
                        refreshData={refreshData}
                        openAssertionNote={openAssertionNote}
                        onOpenTunePredictionsModal={handleOpenTunePredictionsModal}
                    />
                );
            default:
                return (
                    <StatusOverTimeAssertionResultChart
                        chartDimensions={chartDimensions}
                        data={assertionResultChartData}
                        timeRange={timeRange}
                        onOpenTunePredictionsModal={handleOpenTunePredictionsModal}
                    />
                );
        }
    };

    return (
        <VizContainer height={parentDimensions.height} style={{ opacity: isInitializing ? 0 : 1 }}>
            {renderChart()}
            {isTunePredictionsModalOpen && monitor && (
                <TuneSmartAssertionModal
                    onClose={() => setIsTunePredictionsModalOpen(false)}
                    monitor={monitor}
                    assertion={assertion}
                />
            )}
        </VizContainer>
    );
};
