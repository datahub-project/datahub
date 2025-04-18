import { Radio, RadioChangeEvent, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { DEFAULT_DATASET_FRESHNESS_ASSERTION_STATE } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/constants';
import { FixedIntervalScheduleBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/freshness/FixedIntervalSchedulerBuilder';
import {
    FreshnessAssertionBuilderSchedule,
    FreshnessAssertionScheduleBuilderTypeOptions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';
import { useAppConfig } from '@src/app/useAppConfig';

import { FreshnessAssertionScheduleType } from '@types';

const Form = styled.div`
    margin: 16px 0 24px;
`;

const Title = styled(Typography.Title)`
    && {
        margin-bottom: 12px;
    }
`;

const RadioGroup = styled(Radio.Group)`
    display: flex;
    flex-direction: column;
    gap: 8px;
    margin-bottom: 8px;
`;

const RadioContainer = styled.div`
    background-color: ${ANTD_GRAY[1]};
    border: 1px solid ${ANTD_GRAY[5]};
    border-radius: 12px;
    padding: 8px 16px;
    height: 100px;
    display: flex;
    align-items: center;
`;

const StyledRadio = styled(Radio)`
    display: flex;
    align-items: center;
`;

const TextContainer = styled.div`
    margin-left: 4px;
    display: flex;
    flex-direction: column;
    gap: 4px;
`;
type Props = {
    value?: FreshnessAssertionBuilderSchedule;
    onChange: (schedule: FreshnessAssertionBuilderSchedule) => void;
    disabled?: boolean;
    isEditMode?: boolean;
};

export const DatasetFreshnessScheduleBuilder = ({ value, onChange, disabled, isEditMode }: Props) => {
    const { type: scheduleType, fixedInterval } = value ?? {};

    const { onlineSmartAssertionsEnabled } = useAppConfig().config.featureFlags;
    const isDetectWithAIEnabled = !isEditMode && onlineSmartAssertionsEnabled;

    const handleScheduleTypeChange = (newScheduleType: FreshnessAssertionScheduleType) => {
        onChange({
            ...value,
            type: newScheduleType,
            // If we are switching to fixed interval assertion, be sure to set a default interval!
            fixedInterval: value?.fixedInterval ?? DEFAULT_DATASET_FRESHNESS_ASSERTION_STATE.schedule?.fixedInterval,
        });
    };

    const handleFixedIntervalScheduleChange = (
        newFixedSchedule: FreshnessAssertionBuilderSchedule['fixedInterval'],
    ) => {
        onChange({
            ...value,
            fixedInterval: newFixedSchedule,
        });
    };

    return (
        <Form>
            <Title level={5}>Pass if this table has updated...</Title>
            <RadioGroup
                value={scheduleType}
                onChange={(e: RadioChangeEvent) => handleScheduleTypeChange(e.target.value)}
                disabled={disabled}
            >
                <RadioContainer>
                    <StyledRadio value={FreshnessAssertionScheduleBuilderTypeOptions.SinceTheLastCheck}>
                        <TextContainer>
                            <Typography.Text strong>Since the previous check</Typography.Text>
                            <Typography.Text type="secondary">
                                We&apos;ll verify that this table has updated since the last scheduled check.
                            </Typography.Text>
                        </TextContainer>
                    </StyledRadio>
                </RadioContainer>
                <RadioContainer>
                    <StyledRadio value={FreshnessAssertionScheduleBuilderTypeOptions.FixedInterval}>
                        <TextContainer>
                            <FixedIntervalScheduleBuilder
                                value={fixedInterval}
                                onChange={handleFixedIntervalScheduleChange}
                                disabled={disabled}
                                // Prevents clicks inside Select dropdown from malfunctioning when nested inside Radio:
                                stopPropagation
                            />
                            {fixedInterval?.unit && fixedInterval?.multiple && (
                                <Typography.Text type="secondary">
                                    We&apos;ll verify that this table has updated within {fixedInterval?.multiple}{' '}
                                    {fixedInterval?.unit.toLocaleLowerCase()}s of the scheduled check.
                                </Typography.Text>
                            )}
                        </TextContainer>
                    </StyledRadio>
                </RadioContainer>
                {/* Cannot change an imperative freshness assertion to an inferred freshness assertion */}
                {isDetectWithAIEnabled && (
                    <RadioContainer>
                        <StyledRadio value={FreshnessAssertionScheduleBuilderTypeOptions.AiInferred}>
                            <TextContainer>
                                <Typography.Text strong>Detect with AI</Typography.Text>
                                <Typography.Text type="secondary">
                                    We&apos;ll verify that this table updates within the &apos;normal&apos; historical
                                    cadence. If there are any anomalies, the check will fail.
                                </Typography.Text>
                            </TextContainer>
                        </StyledRadio>
                    </RadioContainer>
                )}
            </RadioGroup>
        </Form>
    );
};
