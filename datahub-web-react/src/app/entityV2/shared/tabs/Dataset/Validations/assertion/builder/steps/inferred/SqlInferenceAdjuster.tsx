import { Button, Text, colors } from '@components';
import { Sparkle } from '@phosphor-icons/react';
import { Typography, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { EvaluationScheduleBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/common/EvaluationScheduleBuilder';
import { ExclusionWindowAdjuster } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/ExclusionWindowAdjuster';
import { InferenceSensitivityAdjuster } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/InferenceSensitivityAdjuster';
import { LookBackWindowAdjuster } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/LookBackWindowAdjuster';
import { ENABLE_INFERRED_ASSERTION_PREDICTIONS_TUNING_FORM_ON_SINGLE_CREATE } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/inferred/common/contsants';
import {
    AssertionMonitorBuilderExclusionWindow,
    AssertionMonitorBuilderState,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';
import { TuneSmartAssertionModal } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/tuning/TuneSmartAssertionModal';
import { useAppConfig } from '@src/app/useAppConfig';

import { Assertion, AssertionType, CronSchedule, Maybe, Monitor } from '@types';

const Row = styled.div`
    display: flex;
    flex-direction: column;
    border-style: solid;
    border-width: 0px;
    border-top-width: 1px;
    border-bottom-width: 0;
    border-color: #eee;
    margin-top: 24px;
    margin-bottom: 24px;
    padding-top: 24px;
`;

const TitleWrapper = styled.div`
    display: flex;
    flex-direction: column;
    margin-bottom: 12px;
`;

type Props = {
    state: AssertionMonitorBuilderState;
    updateState: (state: AssertionMonitorBuilderState) => void;
    disabled?: boolean;
    isEditMode?: boolean;
    monitor?: Maybe<Monitor>;
    assertion?: Assertion;
};

export const SqlInferenceAdjuster = ({ state, updateState, disabled, isEditMode, monitor, assertion }: Props) => {
    const { inferenceSettings } = state;
    const { sensitivity, exclusionWindows, trainingDataLookbackWindowDays } = inferenceSettings || {};

    const [isTunePredictionsModalOpen, setIsTunePredictionsModalOpen] = useState(false);

    const { onlineSmartAssertionsEnabled } = useAppConfig().config.featureFlags;

    if (!onlineSmartAssertionsEnabled) return null;

    return (
        <Row>
            {ENABLE_INFERRED_ASSERTION_PREDICTIONS_TUNING_FORM_ON_SINGLE_CREATE || isEditMode ? (
                <>
                    <TitleWrapper>
                        <Typography.Title level={5}>AI Model Tuning</Typography.Title>
                        {!isEditMode && (
                            <Typography.Text style={{ color: colors.gray[400] }}>
                                Consider tuning this after the assertion is up and running.
                            </Typography.Text>
                        )}
                    </TitleWrapper>

                    {isEditMode && monitor && assertion ? (
                        <Button
                            variant="secondary"
                            size="sm"
                            style={{ alignSelf: 'flex-start' }}
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
                    ) : (
                        ENABLE_INFERRED_ASSERTION_PREDICTIONS_TUNING_FORM_ON_SINGLE_CREATE && (
                            <>
                                {/* Sensitivity */}
                                <InferenceSensitivityAdjuster
                                    sensitivity={sensitivity?.level}
                                    disabled={disabled}
                                    onChange={(value) => {
                                        updateState({
                                            ...state,
                                            inferenceSettings: { ...inferenceSettings, sensitivity: { level: value } },
                                        });
                                    }}
                                />

                                {/* Exclusion windows */}
                                <ExclusionWindowAdjuster
                                    exclusionWindows={exclusionWindows || []}
                                    disabled={disabled}
                                    onChange={(value: AssertionMonitorBuilderExclusionWindow) => {
                                        updateState({
                                            ...state,
                                            inferenceSettings: { ...inferenceSettings, exclusionWindows: value },
                                        });
                                    }}
                                />

                                {/* Training data lookback window days */}
                                <LookBackWindowAdjuster
                                    trainingDataLookbackWindowDays={trainingDataLookbackWindowDays}
                                    disabled={disabled}
                                    onChange={(value) => {
                                        updateState({
                                            ...state,
                                            inferenceSettings: {
                                                ...inferenceSettings,
                                                trainingDataLookbackWindowDays: value,
                                            },
                                        });
                                    }}
                                />
                            </>
                        )
                    )}
                </>
            ) : null}

            <EvaluationScheduleBuilder
                value={state.schedule}
                onChange={(newSchedule: CronSchedule) => {
                    updateState({
                        ...state,
                        schedule: newSchedule,
                    });
                }}
                assertionType={AssertionType.Sql}
                showAdvanced={false}
                disabled={disabled}
                headerLabel="Run this query"
            />

            {isEditMode && monitor && assertion && isTunePredictionsModalOpen ? (
                <TuneSmartAssertionModal
                    onClose={() => setIsTunePredictionsModalOpen(false)}
                    monitor={monitor}
                    assertion={assertion}
                />
            ) : null}
        </Row>
    );
};
