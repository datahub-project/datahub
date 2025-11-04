import { Button, Text } from '@components';
import { Typography, message } from 'antd';
import { Sparkle } from 'phosphor-react';
import React, { useState } from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
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
import { getTimeRangeDisplay } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/utils';
import { TuneSmartAssertionModal } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/TuneSmartAssertionModal';

import { Assertion, AssertionRunEventsResult, AssertionRunStatus, AssertionSourceType, Maybe, Monitor } from '@types';

const VIZ_CONTAINER_TITLE_HEIGHT = 36;

const VizContainer = styled.div<{ height: number }>`
    border-radius: 4px;
    height: ${({ height }) => height}px;
    display: flex;
    flex-direction: column;
    align-items: center;
`;

const VizHeader = styled.div`
    width: 100%;
    height: ${VIZ_CONTAINER_TITLE_HEIGHT}px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    width: 100%;
    gap: 12px;
    margin-bottom: 20px;
    margin-top: 4px;
`;

const VizHeaderTitle = styled(Typography.Text)`
    color: ${ANTD_GRAY[9]};
    font-size: 16px;
    font-weight: 600;
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

    const isSmartAssertion = assertion.info?.source?.type === AssertionSourceType.Inferred;

    // render
    const chartDimensions = {
        height: parentDimensions.height - VIZ_CONTAINER_TITLE_HEIGHT - 28, // margin below (flex-start)
        width: parentDimensions.width - 8, // margin on the sides (we have align-items=center)
    };

    const bestChartType = getBestChartTypeForAssertion(assertion.info);

    const renderChartTitle = (title?: string) => (
        <VizHeader>
            <VizHeaderTitle strong>{title || getTimeRangeDisplay(timeRange)}</VizHeaderTitle>
            {isSmartAssertion && bestChartType === AssertionChartType.ValuesOverTime && (
                <Button
                    variant="secondary"
                    size="sm"
                    onClick={() => {
                        if (!monitor) {
                            message.error('Could not find the monitor for this assertion.');
                        } else {
                            setIsTunePredictionsModalOpen(true);
                        }
                    }}
                >
                    <Sparkle weight="fill" size={12} />
                    <Text>Tune Predictions</Text>
                </Button>
            )}
        </VizHeader>
    );

    const renderChart = (): JSX.Element | undefined => {
        switch (bestChartType) {
            case AssertionChartType.ValuesOverTime:
                return (
                    <ValuesOverTimeAssertionResultChart
                        chartDimensions={chartDimensions}
                        data={assertionResultChartData}
                        exclusionWindows={exclusionWindows}
                        timeRange={timeRange}
                        renderHeader={renderChartTitle}
                        refreshData={refreshData}
                        openAssertionNote={openAssertionNote}
                        onTimeRangeChange={onTimeRangeChange}
                    />
                );
            case AssertionChartType.Freshness:
                return (
                    <FreshnessResultChart
                        chartDimensions={chartDimensions}
                        data={assertionResultChartData}
                        exclusionWindows={exclusionWindows}
                        timeRange={timeRange}
                        renderHeader={renderChartTitle}
                        refreshData={refreshData}
                        openAssertionNote={openAssertionNote}
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
