import { Modal, Text, colors } from '@components';
import { Modal as AntdModal, DatePicker, message } from 'antd';
import moment from 'moment';
import { Info, Sparkle } from 'phosphor-react';
import React, { useEffect, useRef, useState } from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { getDatasetUrnFromMonitorUrn } from '@app/entity/shared/utils';
import { DEFAULT_SMART_ASSERTION_TRAINING_LOOKBACK_WINDOW_DAYS } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/LookBackWindowAdjuster';
import { MonitorInferenceSettingsControlPanel } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/MonitorInferenceSettingsControlPanel';
import { TuningHelpBanner } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/TuningHelpBanner';
import {
    AddNamedExclusionWindowModal,
    AddNamedExclusionWindowModalRef,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/chart/AddNamedExclusionWindowModal';
import { DataFreshnessChart } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/chart/freshness/DataFreshnessChart';
import { usePollForNewPredictions } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/poller';
import { getChangedTunePredictionsSettings } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/tunePredictionsAnalytics.utils';
import { LAST_UPDATED_TIMESTAMP_FILTER_NAME, OPERATION_TYPE_FILTER_NAME } from '@app/searchV2/utils/constants';

import { useGetOperationsQuery } from '@graphql/dataset.generated';
import {
    useBulkUpdateAnomaliesMutation,
    useGetAssertionWithMonitorsQuery,
    useListMonitorAnomaliesQuery,
    useUpdateAssertionMonitorSettingsMutation,
} from '@graphql/monitor.generated';
import {
    AnomalyReviewState,
    Assertion,
    AssertionAdjustmentSettings,
    DateInterval,
    FilterOperator,
    Monitor,
    OperationType,
} from '@types';

const HeaderContainer = styled.div`
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    width: 100%;
    gap: 16px;
    margin-top: 8px;
    box-sizing: border-box;
`;

const HeaderLeft = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    gap: 8px;
`;

const TitleRow = styled.div`
    display: flex;
    align-items: center;
    gap: 16px;
`;

const HeaderRight = styled.div`
    display: flex;
    align-items: flex-start;
    justify-content: flex-end;
    flex-shrink: 0;
`;

const PredictionCard = styled.div`
    background: white;
    border-radius: 8px;
    padding: 12px;
    box-shadow: 0px 2px 8px rgba(0, 0, 0, 0.1);
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

const PredictionTitle = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
    font-weight: 700;
    font-size: 12px;
    color: ${colors.gray[900]};
    line-height: 1.2;
`;

const PredictionText = styled(Text)`
    font-size: 14px;
    color: ${colors.gray[500]};
    line-height: 1.2;
`;

const StyledRangePicker = styled(DatePicker.RangePicker)`
    &&& {
        .ant-picker-input > input {
            font-size: 14px;
            color: ${colors.gray[500]};
        }
    }

    border: 1px solid ${colors.gray[100]};
    border-radius: 8px;
    box-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07);
`;

type Props = {
    onClose: () => void;
    assertion: Assertion;
    monitor: Monitor;
};

/**
 * Formats DateInterval enum to a singular/plural string based on the multiple.
 */
const formatDateInterval = (interval: DateInterval, multiple: number): string => {
    const singularMapping: Record<DateInterval, string> = {
        [DateInterval.Second]: 'second',
        [DateInterval.Minute]: 'minute',
        [DateInterval.Hour]: 'hour',
        [DateInterval.Day]: 'day',
        [DateInterval.Week]: 'week',
        [DateInterval.Month]: 'month',
        [DateInterval.Year]: 'year',
    };
    const singular = singularMapping[interval] || interval.toLowerCase();
    return multiple === 1 ? singular : `${singular}s`;
};

/**
 * Extracts the freshness prediction from the assertion's schedule.
 * For freshness assertions, the prediction is stored in the assertion's
 * current schedule (fixedInterval), not in embeddedAssertions.
 */
const extractFreshnessPrediction = (
    assertion: Assertion | null | undefined,
): { multiple: number; unit: string } | null => {
    if (!assertion) return null;

    const { freshnessAssertion } = assertion.info || {};
    const { schedule } = freshnessAssertion || {};

    if (!schedule || schedule.type !== 'FIXED_INTERVAL' || !schedule.fixedInterval) {
        return null;
    }

    const { fixedInterval } = schedule;
    return {
        multiple: fixedInterval.multiple,
        unit: formatDateInterval(fixedInterval.unit, fixedInterval.multiple),
    };
};

export const TuneSmartFreshnessAssertionModal = ({ onClose, assertion, monitor: originalMonitor }: Props) => {
    // -------- Fetch monitor data -------- //
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
    const monitorTrainingLookbackWindowMillis = monitorTrainingLookbackWindowDays * 24 * 60 * 60 * 1000;

    // -------- Extract predictions data -------- //
    const firstAssertion = monitor.info?.assertionMonitor?.assertions?.[0];
    const predictionsGeneratedAt = firstAssertion?.context?.inferenceDetails?.generatedAt;

    // -------- Polling for new predictions after settings update -------- //
    const { isPolling, startPolling } = usePollForNewPredictions(refetchMonitor, predictionsGeneratedAt, 3000);

    // Extract freshness prediction from the assertion's schedule
    // Use the assertion from the query result if available, otherwise fall back to the prop
    const fetchedAssertion = assertionData?.assertion as Assertion | undefined;
    const assertionToUse = fetchedAssertion ?? assertion;
    const nowRef = useRef(Date.now());
    const prediction = extractFreshnessPrediction(assertionToUse);

    // -------- Date range picker state -------- //
    const getOriginalRange = () => ({
        start: nowRef.current - monitorTrainingLookbackWindowMillis,
        end: nowRef.current,
    });
    const [range, setRange] = useState<{ start: number; end: number }>(getOriginalRange());

    // Update range when lookback days change
    useEffect(() => {
        setRange({
            start: nowRef.current - monitorTrainingLookbackWindowMillis,
            end: nowRef.current,
        });
    }, [monitorTrainingLookbackWindowMillis]);

    // -------- Update adjustment settings -------- //
    const [updateAssertionMonitorSettings] = useUpdateAssertionMonitorSettingsMutation();
    const [isUpdating, setIsUpdating] = useState(false);

    const onUpdateAssertionMonitorSettings = async (settings: AssertionAdjustmentSettings) => {
        setIsUpdating(true);
        try {
            await updateAssertionMonitorSettings({
                variables: { input: { urn: monitor.urn, adjustmentSettings: settings } },
            });
            const changedFields = getChangedTunePredictionsSettings(inferenceSettings ?? undefined, settings);
            analytics.event({
                type: EventType.TunePredictionsUpdateMonitorSettingsEvent,
                tuningMode: 'freshness',
                monitorUrn: monitor.urn,
                assertionUrn: assertion.urn,
                datasetUrn: getDatasetUrnFromMonitorUrn(monitor.urn),
                changedFields,
                trainingDataLookbackWindowDays: settings.trainingDataLookbackWindowDays ?? undefined,
                sensitivityLevel: settings.sensitivity?.level ?? undefined,
                exclusionWindowsCount: settings.exclusionWindows?.length ?? 0,
                algorithm: (settings.algorithmName || settings.algorithm) ?? undefined,
            });
            // Start polling for new predictions after successful settings update
            startPolling();
            await refetchMonitor();
        } catch (error) {
            message.error('Failed to update assertion monitor settings');
        } finally {
            setIsUpdating(false);
        }
    };

    const addNamedExclusionWindowModalRef = useRef<AddNamedExclusionWindowModalRef>(null);

    const entityUrn = assertion.info?.entityUrn;

    if (!entityUrn) {
        throw new Error('Entity URN is required to tune freshness assertion.');
    }

    // Compute query range based on current lookback days
    const queryStart = nowRef.current - monitorTrainingLookbackWindowMillis;
    const queryEnd = nowRef.current;

    const { data, loading, error } = useGetOperationsQuery({
        variables: {
            urn: entityUrn,
            filters: {
                and: [
                    {
                        field: LAST_UPDATED_TIMESTAMP_FILTER_NAME,
                        condition: FilterOperator.GreaterThanOrEqualTo,
                        // We use the query range to ensure we get all
                        // operations within the lookback window. Filtering is
                        // done in the chart component.
                        values: [queryStart.toString()],
                    },
                    {
                        field: LAST_UPDATED_TIMESTAMP_FILTER_NAME,
                        condition: FilterOperator.LessThanOrEqualTo,
                        values: [queryEnd.toString()],
                    },
                    {
                        field: OPERATION_TYPE_FILTER_NAME,
                        condition: FilterOperator.In,
                        // We only want to track operations that can be counted
                        // as "FRESHNESS" events, as in there is new data in the
                        // table
                        values: [OperationType.Insert, OperationType.Create, OperationType.Update],
                    },
                ],
            },
        },
    });

    const operations = data?.dataset?.operations || [];

    const { data: anomalyData, refetch: refetchAnomalies } = useListMonitorAnomaliesQuery({
        variables: {
            input: {
                monitorUrn: monitor.urn,
                startTimeMillis: queryStart,
                endTimeMillis: queryEnd,
                filter: {
                    latestBySourceEventOnly: true,
                    states: [AnomalyReviewState.Confirmed],
                },
            },
        },
    });

    const anomalyTimestamps = new Set<number>(
        (anomalyData?.listMonitorAnomalies?.anomalies || [])
            .map((anomaly) => anomaly.source?.sourceEventTimestampMillis)
            .filter((ts): ts is number => !!ts),
    );

    // -------- Bulk mark anomalies -------- //
    const [bulkUpdateAnomalies] = useBulkUpdateAnomaliesMutation();

    const onBulkMarkAnomalies = (startTimeMillis: number, endTimeMillis: number) => {
        AntdModal.confirm({
            title: 'Mark Operation(s) as Anomalous?',
            content:
                'This action will mark all operations in the selected time range as confirmed anomalies. This will help improve future predictions.',
            okText: 'Mark as Anomalous',
            cancelText: 'Cancel',
            icon: <Info size={24} color={colors.gray[900]} />,
            onOk: async () => {
                try {
                    setIsUpdating(true);

                    // Use bulkUpdateAnomalies mutation - the backend will handle finding operations
                    // and creating anomaly events for freshness assertions
                    const result = await bulkUpdateAnomalies({
                        variables: {
                            input: {
                                monitorUrn: monitor.urn,
                                assertionUrn: assertion.urn,
                                startTimeMillis,
                                endTimeMillis,
                                state: AnomalyReviewState.Confirmed,
                            },
                        },
                    });

                    const count = result.data?.bulkUpdateAnomalies?.length || 0;
                    if (count > 0) {
                        message.success(`${count} operation${count > 1 ? 's' : ''} marked as anomalies.`);
                        analytics.event({
                            type: EventType.TunePredictionsMarkAnomalyEvent,
                            tuningMode: 'freshness',
                            action: 'mark',
                            monitorUrn: monitor.urn,
                            assertionUrn: assertion.urn,
                            datasetUrn: getDatasetUrnFromMonitorUrn(monitor.urn),
                            startTimeMillis,
                            endTimeMillis,
                            updatedCount: count,
                            state: AnomalyReviewState.Confirmed,
                        });
                        // Start polling for new predictions after successful bulk update
                        // Refetch will happen automatically when polling completes via onPollingComplete callback
                        startPolling(() => {
                            refetchMonitor();
                            refetchAnomalies();
                        });
                    } else {
                        message.warning('No operations found in the selected time range.');
                    }
                } catch (err) {
                    message.error('Failed to bulk mark anomalies. Please try again later.');
                } finally {
                    setIsUpdating(false);
                }
            },
        });
    };

    // -------- Bulk unmark anomalies -------- //
    const onBulkUnmarkAnomalies = (startTimeMillis: number, endTimeMillis: number) => {
        AntdModal.confirm({
            title: 'Unmark Operation(s) as Anomalous?',
            content:
                'This action will unmark all operations in the selected time range as anomalies. This will help improve future predictions.',
            okText: 'Unmark as Anomalous',
            cancelText: 'Cancel',
            icon: <Info size={24} color={colors.gray[900]} />,
            onOk: async () => {
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

                    const count = result.data?.bulkUpdateAnomalies?.length || 0;
                    if (count > 0) {
                        message.success(`${count} operation${count > 1 ? 's' : ''} unmarked as anomalies.`);
                        analytics.event({
                            type: EventType.TunePredictionsMarkAnomalyEvent,
                            tuningMode: 'freshness',
                            action: 'unmark',
                            monitorUrn: monitor.urn,
                            assertionUrn: assertion.urn,
                            datasetUrn: getDatasetUrnFromMonitorUrn(monitor.urn),
                            startTimeMillis,
                            endTimeMillis,
                            updatedCount: count,
                            state: AnomalyReviewState.Rejected,
                        });
                        // Start polling for new predictions after successful bulk update
                        // Refetch will happen automatically when polling completes via onPollingComplete callback
                        startPolling(() => {
                            refetchMonitor();
                            refetchAnomalies();
                        });
                    } else {
                        message.warning('No anomalies found in the selected time range.');
                    }
                } catch (err) {
                    message.error('Failed to bulk unmark anomalies. Please try again later.');
                } finally {
                    setIsUpdating(false);
                }
            },
        });
    };

    // Convert current range to moment objects for the date picker
    const currentDateRange = [range.start ? moment(range.start) : null, range.end ? moment(range.end) : null] as [
        moment.Moment | null,
        moment.Moment | null,
    ];

    // Handle date range changes from the picker
    const handleDateRangeChange = (dates: [moment.Moment | null, moment.Moment | null] | null) => {
        if (dates && dates[0] && dates[1]) {
            setRange({
                start: dates[0].clone().startOf('day').valueOf(),
                end: dates[1].clone().endOf('day').valueOf(),
            });
        }
    };

    // Check if current range differs from original range
    const originalRange = getOriginalRange();
    const hasRangeChanged = range.start !== originalRange.start || range.end !== originalRange.end;

    const modalTitle = (
        <HeaderContainer>
            <HeaderLeft>
                <TitleRow>
                    <Text type="span" weight="bold" size="lg" color="gray" colorLevel={600}>
                        Tune Predictions
                    </Text>
                    <StyledRangePicker
                        value={currentDateRange}
                        onChange={handleDateRangeChange}
                        format="MMM DD, YYYY"
                        allowClear={false}
                        placeholder={['Start Date', 'End Date']}
                        size="small"
                    />
                </TitleRow>
                <Text type="span" color="gray" colorLevel={1700} weight="medium" size="md">
                    Mark historical anomalies and tune controls to improve predictions.
                </Text>
            </HeaderLeft>
            <HeaderRight>
                {prediction && (
                    <PredictionCard>
                        <PredictionTitle>
                            AI Prediction
                            <Sparkle size={14} color={colors.primary[500]} weight="fill" />
                        </PredictionTitle>
                        <PredictionText>
                            Table should update every {prediction.multiple} {prediction.unit}
                        </PredictionText>
                    </PredictionCard>
                )}
            </HeaderRight>
        </HeaderContainer>
    );

    const isLoading = loading || isUpdating || assertionLoading || isPolling;

    return (
        <Modal
            title={modalTitle}
            width={800}
            open
            onCancel={onClose}
            closable={false}
            bodyStyle={{ maxHeight: '80vh', overflowY: 'auto' }}
            buttons={[
                {
                    text: 'Done',
                    onClick: onClose,
                },
            ]}
        >
            <TuningHelpBanner message="Drag your cursor around anomalous update intervals to exclude them and improve model quality" />
            {error && (
                <div style={{ marginBottom: 16, padding: 12, background: '#fff2f0', borderRadius: 4 }}>
                    <Text color="red" size="sm">
                        {error.message || 'Failed to load dataset update events'}
                    </Text>
                </div>
            )}
            <DataFreshnessChart
                operations={operations}
                loading={isLoading}
                width={750}
                height={400}
                onRangeChange={setRange}
                resetRange={hasRangeChanged ? () => setRange(getOriginalRange()) : undefined}
                currentTime={nowRef.current}
                range={range}
                onBulkMarkAnomalies={onBulkMarkAnomalies}
                onBulkUnmarkAnomalies={onBulkUnmarkAnomalies}
                anomalyTimestamps={anomalyTimestamps}
            />
            <MonitorInferenceSettingsControlPanel
                onUpdateSettings={onUpdateAssertionMonitorSettings}
                inferenceSettings={inferenceSettings ?? undefined}
                isUpdating={isLoading}
            />
            <AddNamedExclusionWindowModal
                ref={addNamedExclusionWindowModalRef}
                inferenceSettings={inferenceSettings ?? {}}
                onUpdateAssertionMonitorSettings={onUpdateAssertionMonitorSettings}
            />
        </Modal>
    );
};
