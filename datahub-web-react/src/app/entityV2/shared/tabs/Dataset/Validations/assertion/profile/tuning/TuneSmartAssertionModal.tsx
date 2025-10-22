import { Modal, Text, colors } from '@components';
import { Modal as AntdModal, message } from 'antd';
import moment from 'moment';
import { Info } from 'phosphor-react';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import styled from 'styled-components';

import { DEFAULT_SMART_ASSERTION_TRAINING_LOOKBACK_WINDOW_DAYS } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/LookBackWindowAdjuster';
import {
    AssertionPrediction,
    extractPredictionsFromEmbeddedAssertions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/useInferenceRegenerationPoller';
import { MonitorInferenceSettingsControlPanel } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/MonitorInferenceSettingsControlPanel';
import { transformData } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/TuneSmartAssertionModal.utils';
import { TuningHelpBanner } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/TuningHelpBanner';
import {
    AddNamedExclusionWindowModal,
    AddNamedExclusionWindowModalRef,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/chart/AddNamedExclusionWindowModal';
import { MonitorMetricsChart } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/chart/MonitorMetricsChart';
import { usePollForNewPredictions } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/poller';
import Loading from '@app/shared/Loading';

import {
    useBulkUpdateAnomaliesMutation,
    useGetAssertionWithMonitorsQuery,
    useListMonitorMetricsQuery,
    useUpdateAssertionMonitorSettingsMutation,
} from '@graphql/monitor.generated';
import { AnomalyReviewState, Assertion, AssertionAdjustmentSettings, AssertionExclusionWindow, Monitor } from '@types';

const RegeneratingContainer = styled.div`
    display: flex;
    align-items: center;
    margin-bottom: 16px;
    font-size: 12px;
    color: ${colors.gray[6]};
`;

const LoadingWrapper = styled.div`
    margin-right: 8px;
`;

const DEFAULT_PREDICTION_LOOKAHEAD_DAYS = 2;
const DEFAULT_PREDICTION_LOOKAHEAD_TIME_MILLIS = DEFAULT_PREDICTION_LOOKAHEAD_DAYS * 24 * 60 * 60 * 1000;

type Props = {
    onClose: () => void;
    monitor: Monitor;
    assertion: Assertion;
};

export const TuneSmartAssertionModal = ({ onClose, monitor: originalMonitor, assertion }: Props) => {
    const {
        data: assertionData,
        loading: assertionLoading,
        refetch: refetchMonitor,
    } = useGetAssertionWithMonitorsQuery({ variables: { assertionUrn: assertion.urn } });
    const fetchedMonitor = assertionData?.assertion?.monitor?.relationships?.[0]?.entity as Monitor;
    const monitor = fetchedMonitor ?? originalMonitor;

    // -------- Extract tuning settings data -------- //
    const inferenceSettings = monitor.info?.assertionMonitor?.settings?.inferenceSettings;
    const monitorTrainingLookbackWindowDays =
        inferenceSettings?.trainingDataLookbackWindowDays ?? DEFAULT_SMART_ASSERTION_TRAINING_LOOKBACK_WINDOW_DAYS;

    const monitorExclusionWindows: AssertionExclusionWindow[] = useMemo(
        () => inferenceSettings?.exclusionWindows ?? [],
        [inferenceSettings],
    );

    // -------- Extract predictions data -------- //
    const embeddedAssertions = useMemo(
        () => monitor.info?.assertionMonitor?.assertions?.[0].context?.embeddedAssertions ?? [],
        [monitor],
    );
    const predictionsGeneratedAt =
        monitor.info?.assertionMonitor?.assertions?.[0].context?.inferenceDetails?.generatedAt;

    // -------- Polling for new predictions after settings update -------- //
    const { isPolling, startPolling } = usePollForNewPredictions(refetchMonitor, predictionsGeneratedAt);

    // Get all predictions from embeddedAssertions
    const predictions: AssertionPrediction[] = useMemo(() => {
        return extractPredictionsFromEmbeddedAssertions(embeddedAssertions);
    }, [embeddedAssertions]);

    // -------- Date range picker state -------- //
    const nowRef = useRef(Date.now());
    const originalRangeRef = useRef({
        start: nowRef.current - monitorTrainingLookbackWindowDays * 24 * 60 * 60 * 1000,
        end: nowRef.current + DEFAULT_PREDICTION_LOOKAHEAD_TIME_MILLIS,
    });
    const [range, setRange] = useState<{ start: number; end: number }>(originalRangeRef.current);

    // -------- Metrics data -------- //
    const {
        data,
        loading,
        refetch: refetchListMonitorMetrics,
    } = useListMonitorMetricsQuery({
        variables: {
            input: {
                monitorUrn: monitor.urn,
                startTimeMillis: range.start,
                endTimeMillis: range.end,
            },
        },
    });
    const metrics = useMemo(() => transformData(data), [data]);

    // Cache the latest metric timestamp
    const latestMetricTimestampRef = useRef(nowRef.current);

    // -------- Update adjustment settings -------- //
    const [updateAssertionMonitorSettings] = useUpdateAssertionMonitorSettingsMutation();

    const [isUpdating, setIsUpdating] = useState(false);

    const onUpdateAssertionMonitorSettings = useCallback(
        async (settings: AssertionAdjustmentSettings) => {
            setIsUpdating(true);
            try {
                await updateAssertionMonitorSettings({
                    variables: { input: { urn: monitor.urn, adjustmentSettings: settings } },
                });
                // Start polling for new predictions after successful settings update
                startPolling();
                await refetchMonitor();
            } catch (error) {
                console.error('Error updating assertion monitor settings:', error);
                message.error('Failed to update assertion monitor settings');
            } finally {
                setIsUpdating(false);
            }
        },
        [monitor.urn, updateAssertionMonitorSettings, refetchMonitor, startPolling],
    );

    const [bulkUpdateAnomalies] = useBulkUpdateAnomaliesMutation();

    const executeBulkUnmarkAnomalies = useCallback(
        async (startTimeMillis: number, endTimeMillis: number) => {
            try {
                setIsUpdating(true);
                const result = await bulkUpdateAnomalies({
                    variables: {
                        input: {
                            monitorUrn: monitor.urn,
                            assertionUrn: assertion.urn,
                            startTimeMillis,
                            endTimeMillis,
                            state: AnomalyReviewState.Rejected,
                        },
                    },
                });
                const count = result.data?.bulkUpdateAnomalies?.length;
                message.success(`${count} anomalies bulk updated successfully.`);
                // Start polling for new predictions after successful bulk update
                startPolling();
                setTimeout(() => refetchListMonitorMetrics(), 3000);
                await refetchMonitor();
            } catch (error) {
                console.error('Error bulk updating anomalies:', error);
                message.error('Failed to bulk update anomalies. Please try again later.');
            } finally {
                setIsUpdating(false);
            }
        },
        [monitor.urn, assertion.urn, bulkUpdateAnomalies, startPolling, refetchMonitor, refetchListMonitorMetrics],
    );

    const onBulkUnmarkAnomalies = useCallback(
        (startTimeMillis: number, endTimeMillis: number) => {
            AntdModal.confirm({
                title: 'Confirm Bulk Unmark Anomalies',
                content:
                    'This action will mark all data points in the selected time range as not anomalies. You can always add an exclusion window later.',
                okText: 'Yes, Unmark',
                cancelText: 'Cancel',
                icon: <Info size={24} color={colors.gray[900]} />,
                onOk: () => executeBulkUnmarkAnomalies(startTimeMillis, endTimeMillis),
            });
        },
        [executeBulkUnmarkAnomalies],
    );

    const addNamedExclusionWindowModalRef = useRef<AddNamedExclusionWindowModalRef>(null);

    // -------- Date range event handlers -------- //
    // For the very first time data loads, update the range to fit the timespan of the data
    const isRangeInitializedRef = useRef(false);
    useEffect(() => {
        if (
            data &&
            data.listMonitorMetrics &&
            data.listMonitorMetrics.metrics.length > 0 &&
            !isRangeInitializedRef.current
        ) {
            const sortedMetrics = data.listMonitorMetrics.metrics.sort(
                (a, b) => a.assertionMetric.timestampMillis - b.assertionMetric.timestampMillis,
            );
            originalRangeRef.current = {
                start: sortedMetrics[0].assertionMetric.timestampMillis,
                end: nowRef.current + DEFAULT_PREDICTION_LOOKAHEAD_TIME_MILLIS,
            };
            latestMetricTimestampRef.current =
                sortedMetrics[sortedMetrics.length - 1].assertionMetric.timestampMillis || nowRef.current;
            setRange(originalRangeRef.current);
            isRangeInitializedRef.current = true;
        }
    }, [data]);

    // Convert current range to moment objects for the date picker
    const currentDateRange = useMemo((): [moment.Moment | null, moment.Moment | null] => {
        return [range.start ? moment(range.start) : null, range.end ? moment(range.end) : null];
    }, [range.start, range.end]);

    // Handle date range changes from the picker
    const handleDateRangeChange = (startDate: moment.Moment | null, endDate: moment.Moment | null) => {
        if (startDate && endDate) {
            setRange({
                start: startDate.clone().startOf('day').valueOf(),
                end: endDate.clone().endOf('day').valueOf(),
            });
        }
    };

    // Check if current range differs from original range
    const hasRangeChanged =
        range.start !== originalRangeRef.current.start || range.end !== originalRangeRef.current.end;

    return (
        <Modal
            title="Tune Predictions"
            onCancel={onClose}
            subtitle="Adjust key controls on smart assertions to improve predictions."
            width={800}
            open
            style={{ margin: `16px 0` }}
            bodyStyle={{ maxHeight: '80vh', overflowY: 'auto' }}
            buttons={[
                {
                    text: 'Done',
                    onClick: onClose,
                },
            ]}
        >
            <TuningHelpBanner />
            {/* Show regenerating predictions indicator while polling */}
            {isPolling && (
                <RegeneratingContainer>
                    <LoadingWrapper>
                        <Loading marginTop={0} height={16} />
                    </LoadingWrapper>
                    <Text size="md" color="primary">
                        Regenerating future predictions...
                    </Text>
                </RegeneratingContainer>
            )}
            <MonitorMetricsChart
                metrics={metrics}
                exclusionWindows={metrics.length ? monitorExclusionWindows : []}
                predictions={predictions}
                loading={loading || isUpdating || assertionLoading || isPolling}
                width={750}
                height={400}
                onRangeChange={setRange}
                resetRange={hasRangeChanged ? () => setRange(originalRangeRef.current) : undefined}
                onDateRangeChange={handleDateRangeChange}
                onAddExclusionWindow={(startTimeMillis, endTimeMillis) =>
                    addNamedExclusionWindowModalRef.current?.create({
                        startTimeMillis,
                        endTimeMillis,
                    })
                }
                onBulkUnmarkAnomalies={onBulkUnmarkAnomalies}
                dateRange={currentDateRange}
                currentTime={nowRef.current}
                latestMetricTimestamp={latestMetricTimestampRef.current}
            />
            {/* Settings controls panel */}
            <MonitorInferenceSettingsControlPanel
                onUpdateSettings={onUpdateAssertionMonitorSettings}
                inferenceSettings={inferenceSettings ?? undefined}
                isUpdating={isUpdating}
            />
            {/* Add named exclusion window modal */}
            <AddNamedExclusionWindowModal
                ref={addNamedExclusionWindowModalRef}
                inferenceSettings={inferenceSettings ?? {}}
                onUpdateAssertionMonitorSettings={onUpdateAssertionMonitorSettings}
            />
        </Modal>
    );
};
