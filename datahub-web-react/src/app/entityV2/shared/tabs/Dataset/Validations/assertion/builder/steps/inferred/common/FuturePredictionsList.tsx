import { Button, Card, Icon, Loader, Text, Tooltip } from '@components';
import { Typography } from 'antd';
import React, { forwardRef, useEffect, useImperativeHandle, useRef, useState } from 'react';
import styled from 'styled-components';

import { colors } from '@components/theme';

import {
    extractAssertionData,
    extractPredictionsFromEmbeddedVolumeAssertions,
    useInferenceRegenerationPoller,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/useInferenceRegenerationPoller';
import { AssertionMonitorBuilderState } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';
import { toLocalDateTimeString, toRelativeTimeString } from '@app/shared/time/timeUtils';

import { useGetAssertionWithMonitorsQuery } from '@graphql/monitor.generated';

const PredictionRow = styled.div`
    display: flex;
    flex-direction: column;
    margin-bottom: 0px;
    padding: 0;
    background-color: white;
`;

const SaveMessage = styled.div`
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    display: flex;
    align-items: center;
    justify-content: center;
    background-color: ${colors.gray[0]}CC;
    z-index: 1;
`;

const SaveMessageText = styled.div<{ isTimeout?: boolean }>`
    background-color: ${(props) => (props.isTimeout ? 'white' : colors.gray[1600])};
    padding: 12px 16px;
    border-radius: 8px;
    display: flex;
    align-items: center;
    gap: 8px;
`;

const PredictionsContainer = styled.div`
    position: relative;
    margin-bottom: 12px;
`;

const DEFAULT_PREDICTIONS_SHOWN_COUNT = 3;

/**
 * Starts the regeneration process
 * @param isRegenerating - Whether the regeneration is currently in progress
 * @param previousGeneratedAt - The timestamp of the previous generation
 * @param generatedAt - The timestamp of the current generation
 * @param setIsRegenerating - Function to set the regeneration state
 * @param setHasTimedOut - Function to set the timeout state
 */
const startRegenerating = (
    isRegenerating: boolean,
    previousGeneratedAt: React.MutableRefObject<number | undefined>,
    generatedAt: number,
    setIsRegenerating: React.Dispatch<React.SetStateAction<boolean>>,
    setHasTimedOut: React.Dispatch<React.SetStateAction<boolean>>,
) => {
    if (!isRegenerating) {
        // eslint-disable-next-line no-param-reassign
        previousGeneratedAt.current = generatedAt;
        setIsRegenerating(true);
        setHasTimedOut(false);
    }
};

/**
 * Props for the FuturePredictionsList component
 * @param state - The current state of the assertion monitor builder
 * @param onSave - Optional callback function triggered on save
 */
type Props = {
    state: AssertionMonitorBuilderState;
    onSave?: () => void;
};

/**
 * Interface for the VolumeInferenceAdjuster component
 * @param triggerRegeneration - Function to trigger the regeneration of predictions
 */
export interface VolumeInferenceAdjusterHandle {
    triggerRegeneration: () => void;
}

export const FuturePredictionsList = forwardRef<VolumeInferenceAdjusterHandle, Props>((props, ref) => {
    const { state } = props;

    const [isExpanded, setIsExpanded] = useState(false);
    const [initialSettings, setInitialSettings] = useState(state.inferenceSettings);
    const [isRegenerating, setIsRegenerating] = useState(false);
    const [hasTimedOut, setHasTimedOut] = useState(false);
    const [previousPredictions, setPreviousPredictions] = useState<
        Array<{
            index: number;
            lowerBound?: number;
            upperBound?: number;
            timeWindow?: { startTimeMillis?: number; endTimeMillis?: number };
        }>
    >([]);
    const previousGeneratedAt = useRef<number | undefined>();

    const urn = state?.assertion?.urn as string;
    const { data, refetch } = useGetAssertionWithMonitorsQuery({ variables: { assertionUrn: urn } });
    const { embeddedAssertions, generatedAt } = extractAssertionData(data ?? { __typename: 'Query' });

    useInferenceRegenerationPoller({
        isRegenerating,
        generatedAt,
        refetch,
        previousGeneratedAt,
        setPreviousPredictions,
        setIsRegenerating,
        setHasTimedOut,
        setInitialSettings,
        state,
    });

    // Get all predictions from embeddedAssertions
    const predictions = React.useMemo(() => {
        return extractPredictionsFromEmbeddedVolumeAssertions(embeddedAssertions);
    }, [embeddedAssertions]);

    // Update previous predictions when we have new ones
    useEffect(() => {
        if (predictions.length > 0 && !isRegenerating) {
            setPreviousPredictions(predictions);
        }
    }, [predictions, isRegenerating, setPreviousPredictions]);

    const totalPredictionTimeDeltaHours = React.useMemo(() => {
        // This is so we can continue display previous predictions behind the isRegenerating state instead of no
        // predictions, hopefully less jarring than going from prev predictions -> no predictions -> new predictions:
        const currentPredictions = isRegenerating ? previousPredictions : predictions;
        if (!currentPredictions || currentPredictions.length === 0) return 0;

        const startTime = currentPredictions[0].timeWindow?.startTimeMillis || 0;
        const endTime = currentPredictions[currentPredictions.length - 1].timeWindow?.endTimeMillis || 0;
        return Math.floor((endTime - startTime) / (1000 * 60 * 60));
    }, [isRegenerating, previousPredictions, predictions]);

    // Check if any settings have changed
    const hasSettingsChanged = React.useMemo(() => {
        if (!initialSettings || !state.inferenceSettings) return false;
        return (
            initialSettings.sensitivity?.level !== state.inferenceSettings.sensitivity?.level ||
            initialSettings.trainingDataLookbackWindowDays !== state.inferenceSettings.trainingDataLookbackWindowDays ||
            JSON.stringify(initialSettings.exclusionWindows) !==
                JSON.stringify(state.inferenceSettings.exclusionWindows)
        );
    }, [initialSettings, state.inferenceSettings]);

    const predictionsCount = React.useMemo(() => {
        return (isRegenerating ? previousPredictions : predictions).length;
    }, [isRegenerating, previousPredictions, predictions]);

    const predictionsToShow = React.useMemo(() => {
        const currentPredictions = isRegenerating ? previousPredictions : predictions;
        const numPredictionsToShow = isExpanded ? predictionsCount : DEFAULT_PREDICTIONS_SHOWN_COUNT;
        return currentPredictions.slice(0, numPredictionsToShow);
    }, [isExpanded, isRegenerating, previousPredictions, predictions, predictionsCount]);

    useImperativeHandle(ref, () => ({
        triggerRegeneration: () => {
            startRegenerating(isRegenerating, previousGeneratedAt, generatedAt, setIsRegenerating, setHasTimedOut);
        },
    }));

    const handleSave = () => {
        if (props.onSave) {
            startRegenerating(isRegenerating, previousGeneratedAt, generatedAt, setIsRegenerating, setHasTimedOut);
            props.onSave();
        }
    };

    return (
        <PredictionsContainer>
            <Card
                title={
                    <Typography.Title level={5} style={{ marginBottom: 0 }}>
                        {predictionsCount > 0 ? 'Predictions' : 'No predictions available'}
                    </Typography.Title>
                }
                subTitle={
                    <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                        {predictionsCount > 0
                            ? `Next ${totalPredictionTimeDeltaHours} hour${totalPredictionTimeDeltaHours !== 1 ? 's' : ''} of predictions${generatedAt ? ` (Generated ${toRelativeTimeString(generatedAt) || 'now'})` : ''}`
                            : 'Once training is complete, you will see predictions here.'}
                    </Typography.Text>
                }
            >
                {predictionsCount > 0 && (
                    <>
                        {predictionsToShow.map((prediction) => (
                            <PredictionRow key={prediction.index}>
                                <Text lineHeight="none">
                                    Row count should be between{' '}
                                    {Number(prediction.lowerBound)?.toLocaleString(undefined, {
                                        maximumFractionDigits: 3,
                                    })}{' '}
                                    and{' '}
                                    {Number(prediction.upperBound)?.toLocaleString(undefined, {
                                        maximumFractionDigits: 3,
                                    })}
                                    {prediction.timeWindow?.startTimeMillis ? (
                                        <Text color="gray">
                                            (
                                            <Tooltip
                                                title={toLocalDateTimeString(prediction.timeWindow.startTimeMillis)}
                                            >
                                                {toRelativeTimeString(prediction.timeWindow.startTimeMillis) || 'now'}
                                            </Tooltip>
                                            )
                                        </Text>
                                    ) : null}
                                </Text>
                            </PredictionRow>
                        ))}
                        {predictionsCount > DEFAULT_PREDICTIONS_SHOWN_COUNT && (
                            <Button variant="text" onClick={() => setIsExpanded(!isExpanded)}>
                                {isExpanded
                                    ? 'Show less'
                                    : `+${predictionsCount - DEFAULT_PREDICTIONS_SHOWN_COUNT} more`}
                            </Button>
                        )}
                    </>
                )}
            </Card>
            {hasSettingsChanged && !isRegenerating && !hasTimedOut && (
                <SaveMessage>
                    <SaveMessageText>
                        <Text>
                            {/* eslint-disable-next-line jsx-a11y/anchor-is-valid */}
                            <a
                                href="#"
                                onClick={(e) => {
                                    e.preventDefault();
                                    handleSave();
                                }}
                            >
                                <code>Save changes</code>
                            </a>{' '}
                            to see updated predictions.
                        </Text>
                    </SaveMessageText>
                </SaveMessage>
            )}
            {isRegenerating && !hasTimedOut && (
                <SaveMessage>
                    <SaveMessageText isTimeout>
                        <Text>
                            <Loader size="sm" /> Regenerating predictions...
                        </Text>
                    </SaveMessageText>
                </SaveMessage>
            )}
            {hasTimedOut && (
                <SaveMessage>
                    <SaveMessageText>
                        <Icon icon="Warning" />
                        <Text>
                            Timed out while waiting for training to complete.
                            <br />
                            Please check back within the hour.
                        </Text>
                    </SaveMessageText>
                </SaveMessage>
            )}
        </PredictionsContainer>
    );
});
