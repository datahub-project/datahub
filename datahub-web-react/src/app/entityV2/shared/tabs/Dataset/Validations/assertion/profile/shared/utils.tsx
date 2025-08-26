import { Modal, colors } from '@components';
import { Typography, message } from 'antd';
import { Info } from 'phosphor-react';
import React, { useCallback, useState } from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { getDatasetUrnFromMonitorUrn } from '@app/entity/shared/utils';
import { removeNestedTypeNames } from '@app/shared/subscribe/drawer/utils';

import {
    useReportAnomalyFeedbackMutation,
    useUpdateAssertionMonitorSettingsMutation,
} from '@graphql/monitor.generated';
import {
    AnomalyReviewState,
    Assertion,
    AssertionExclusionWindowType,
    AssertionRunEvent,
    CronSchedule,
    Monitor,
} from '@types';

const InfoBox = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    background-color: ${colors.gray[1000]};
    padding: 12px;
    border-radius: 8px;
    margin-top: 12px;
`;

export const toReadableLocalDateTimeString = (timeMs: number) => {
    const date = new Date(timeMs);
    return date.toLocaleString([], {
        year: 'numeric',
        month: 'short',
        day: 'numeric',
        hour: 'numeric',
        minute: '2-digit',
        timeZoneName: 'short',
    });
};

export const tryGetScheduleFromMonitor = (monitor?: Monitor): CronSchedule | undefined => {
    return monitor?.info?.assertionMonitor?.assertions?.length
        ? monitor?.info?.assertionMonitor?.assertions[0]?.schedule
        : undefined;
};

const getInferenceDetails = (monitor?: Monitor) => {
    const monitorInferenceSettings = monitor?.info?.assertionMonitor?.settings?.inferenceSettings;
    return {
        sensitivity: monitorInferenceSettings?.sensitivity?.level ?? undefined,
        hasExclusionWindows: (monitorInferenceSettings?.exclusionWindows?.length ?? 0) > 0,
        lookbackDays: monitorInferenceSettings?.trainingDataLookbackWindowDays ?? undefined,
    };
};

const MessageWrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
    text-align: left;
    padding-bottom: 12px;

    h5.ant-typography {
        margin: 0;
    }
    div.ant-typography {
        font-weight: 400;
        margin: 0;
    }
`;

/**
 * Feedback actions for a smart assertion result.
 * This includes:
 * - Marking as anomaly
 * - Unmarking as anomaly
 * - Retraining from today
 * - Toggling anomaly status
 * - Processing state
 * - Retrain modal
 */
export const useAssertionFeedbackActions = ({
    assertion,
    monitor,
    run,
    isAnomaly,
    refetchResults,
}: {
    assertion: Assertion;
    monitor?: Monitor;
    run?: AssertionRunEvent;
    isAnomaly?: boolean;
    refetchResults?: () => Promise<unknown>;
}) => {
    const sleepAndRefetchResults = useCallback(async () => {
        await new Promise((resolve) => {
            setTimeout(() => {
                refetchResults?.();
                resolve(undefined);
            }, 3000);
        });
    }, [refetchResults]);

    // ------------------------------------------------------------------------------------------
    // REPORT AND UPDATE ANOMALY BASE-ACTIONS
    // ------------------------------------------------------------------------------------------
    const [reportAnomalyFeedback] = useReportAnomalyFeedbackMutation();
    const [updateAssertionMonitorSettings] = useUpdateAssertionMonitorSettingsMutation();

    const onMarkAsAnomaly = useCallback(async () => {
        const monitorUrn = monitor?.urn;
        if (!monitorUrn) {
            throw new Error('Monitor not found for this assertion. Please contact support.');
        }
        if (!run) {
            throw new Error('Run not found for this assertion. Please refresh the page and try again.');
        }
        await reportAnomalyFeedback({
            variables: {
                input: {
                    monitorUrn,
                    assertionUrn: assertion.urn,
                    runEventTimestampMillis: run.timestampMillis,
                    state: AnomalyReviewState.Confirmed,
                },
            },
        });
    }, [assertion, monitor, run, reportAnomalyFeedback]);

    const onUnmarkAsAnomaly = useCallback(async () => {
        const monitorUrn = monitor?.urn;
        if (!monitorUrn) {
            throw new Error('Monitor not found for this assertion. Please contact support.');
        }
        if (!run) {
            throw new Error('Run not found for this assertion. Please refresh the page and try again.');
        }
        await reportAnomalyFeedback({
            variables: {
                input: {
                    monitorUrn,
                    assertionUrn: assertion.urn,
                    runEventTimestampMillis: run.timestampMillis,
                    state: AnomalyReviewState.Rejected,
                },
            },
        });
    }, [assertion, monitor, run, reportAnomalyFeedback]);

    // ------------------------------------------------------------------------------------------
    // MARK / UNMARK AS ANOMALY PARENT ACTION
    // ------------------------------------------------------------------------------------------
    const [isActionProcessing, setIsActionProcessing] = useState(false);
    const onToggleAnomaly = useCallback(
        async (isUndo: boolean) => {
            setIsActionProcessing(true);
            try {
                if (isAnomaly) {
                    await onUnmarkAsAnomaly?.();
                    message.info(
                        <MessageWrapper>
                            <Typography.Title level={5}>Marked as Normal. Retraining started.</Typography.Title>
                            <Typography.Paragraph>
                                Also consider <b>decreasing sensitivity</b> and <b>adding exclusion windows</b> in the
                                &apos;Settings&apos; tab.
                            </Typography.Paragraph>
                        </MessageWrapper>,
                        8,
                    );
                    const inferenceDetails = getInferenceDetails(monitor);
                    const runEventTimeMillisFromNow = run ? Date.now() - run.timestampMillis : undefined;
                    if (isUndo) {
                        analytics.event({
                            type: EventType.UndoAnomalyFeedback,
                            assertionType: assertion.info?.type ?? 'Unknown',
                            runEventTimeMillisFromNow,
                            inferenceDetails,
                            datasetUrn: getDatasetUrnFromMonitorUrn(monitor?.urn),
                        });
                    } else {
                        analytics.event({
                            type: EventType.GiveAnomalyFeedback,
                            feedbackType: 'falseAlarm',
                            assertionType: assertion.info?.type ?? 'Unknown',
                            runEventTimeMillisFromNow,
                            inferenceDetails,
                            datasetUrn: getDatasetUrnFromMonitorUrn(monitor?.urn),
                        });
                    }
                } else {
                    await onMarkAsAnomaly?.();
                    message.info(
                        <MessageWrapper>
                            <Typography.Title level={5}>Marked as an Anomaly. Retraining started.</Typography.Title>
                            <Typography.Paragraph>
                                Also consider <b>increasing sensitivity</b> and <b>adding exclusion windows</b> in the
                                &apos;Settings&apos; tab.
                            </Typography.Paragraph>
                        </MessageWrapper>,
                        8,
                    );
                    const inferenceDetails = getInferenceDetails(monitor);
                    const runEventTimeMillisFromNow = run ? Date.now() - run.timestampMillis : undefined;
                    if (isUndo) {
                        analytics.event({
                            type: EventType.UndoAnomalyFeedback,
                            assertionType: assertion.info?.type ?? 'Unknown',
                            runEventTimeMillisFromNow,
                            inferenceDetails,
                            datasetUrn: getDatasetUrnFromMonitorUrn(monitor?.urn),
                        });
                    } else {
                        analytics.event({
                            type: EventType.GiveAnomalyFeedback,
                            feedbackType: 'missedAlarm',
                            assertionType: assertion.info?.type ?? 'Unknown',
                            runEventTimeMillisFromNow,
                            inferenceDetails,
                            datasetUrn: getDatasetUrnFromMonitorUrn(monitor?.urn),
                        });
                    }
                }
                await sleepAndRefetchResults();
            } catch (error: any) {
                message.error((error as Error).message);
            } finally {
                setIsActionProcessing(false);
            }
        },
        [assertion, isAnomaly, monitor, onMarkAsAnomaly, onUnmarkAsAnomaly, run, sleepAndRefetchResults],
    );

    // ------------------------------------------------------------------------------------------
    // RETRAIN AS NEW NORMAL ACTION AND MODAL
    // ------------------------------------------------------------------------------------------
    const [retrainModalOpen, setRetrainModalOpen] = useState(false);
    const onRetrainAsNewNormal = useCallback(() => {
        setRetrainModalOpen(true);
    }, []);

    const retrainFromRun = useCallback(async () => {
        setIsActionProcessing(true);
        try {
            if (!run) {
                throw new Error('Run not found for this assertion. Please refresh the page and try again.');
            }
            if (!monitor) {
                throw new Error('Monitor not found for this assertion. Please contact support.');
            }
            const existingAdjustmentSettings = removeNestedTypeNames(
                monitor?.info?.assertionMonitor?.settings?.inferenceSettings,
            );

            await updateAssertionMonitorSettings({
                variables: {
                    input: {
                        urn: monitor.urn,
                        adjustmentSettings: {
                            ...existingAdjustmentSettings,
                            exclusionWindows: [
                                ...(existingAdjustmentSettings?.exclusionWindows ?? []),
                                {
                                    displayName: `Retrained as new normal from ${toReadableLocalDateTimeString(run.timestampMillis)}`,
                                    type: AssertionExclusionWindowType.FixedRange,
                                    fixedRange: {
                                        startTimeMillis: 0,
                                        endTimeMillis: run.timestampMillis,
                                    },
                                },
                            ],
                        },
                    },
                },
            });

            message.success(
                <MessageWrapper>
                    <Typography.Title level={5}>Excluded all training data before this run.</Typography.Title>
                    <Typography.Paragraph>
                        Retraining in progress. Open the <b>Settings</b> tab to view the exclusion windows.
                    </Typography.Paragraph>
                </MessageWrapper>,
            );
            setRetrainModalOpen(false);
            await sleepAndRefetchResults();

            const inferenceDetails = getInferenceDetails(monitor);
            const runEventTimeMillisFromNow = Date.now() - run.timestampMillis;
            analytics.event({
                type: EventType.RetrainAsNewNormal,
                assertionType: assertion.info?.type ?? 'Unknown',
                runEventTimeMillisFromNow,
                inferenceDetails,
                datasetUrn: getDatasetUrnFromMonitorUrn(monitor?.urn),
            });
        } catch (error: any) {
            message.error((error as Error).message);
        } finally {
            setIsActionProcessing(false);
        }
    }, [assertion, monitor, run, sleepAndRefetchResults, updateAssertionMonitorSettings]);

    const retrainModal = retrainModalOpen && (
        <Modal
            zIndex={9999}
            title="Train as New Normal"
            onCancel={() => setRetrainModalOpen(false)}
            buttons={[
                {
                    variant: 'text',
                    text: 'Cancel',
                    onClick: () => setRetrainModalOpen(false),
                },
                {
                    text: 'Re-Train from this Run',
                    onClick: retrainFromRun,
                    isLoading: isActionProcessing,
                },
            ]}
        >
            <Typography.Text style={{ fontSize: 14 }}>
                All training data before this run will be excluded.
            </Typography.Text>
            <InfoBox>
                <Info size={24} color={colors.gray[900]} />
                <Typography.Text style={{ fontSize: 14, color: colors.gray[900] }}>
                    For more control, set <b>Exclusion Windows</b> in the Settings tab.
                </Typography.Text>
            </InfoBox>
        </Modal>
    );

    return {
        onMarkAsAnomaly,
        onUnmarkAsAnomaly,
        onToggleAnomaly,
        isActionProcessing,
        onRetrainAsNewNormal,
        retrainModal,
    };
};
