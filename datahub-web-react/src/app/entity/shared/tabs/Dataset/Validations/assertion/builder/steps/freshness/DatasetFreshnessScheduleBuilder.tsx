import { Radio, RadioChangeEvent, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { DEFAULT_DATASET_FRESHNESS_ASSERTION_STATE } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/constants';
import { FixedIntervalScheduleBuilder } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/freshness/FixedIntervalSchedulerBuilder';

import { FixedIntervalSchedule, FreshnessAssertionSchedule, FreshnessAssertionScheduleType } from '@types';

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
    value: FreshnessAssertionSchedule;
    onChange: (schedule: FreshnessAssertionSchedule) => void;
    disabled?: boolean;
};

export const DatasetFreshnessScheduleBuilder = ({ value, onChange, disabled }: Props) => {
    const { type: scheduleType, fixedInterval } = value;

    const handleScheduleTypeChange = (newScheduleType: FreshnessAssertionScheduleType) => {
        onChange({
            ...value,
            type: newScheduleType,
            // If we are switching to fixed interval assertion, be sure to set a default interval!
            fixedInterval: value.fixedInterval ?? DEFAULT_DATASET_FRESHNESS_ASSERTION_STATE.schedule?.fixedInterval,
        });
    };

    const handleFixedIntervalScheduleChange = (newFixedSchedule: FixedIntervalSchedule) => {
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
                    <StyledRadio value={FreshnessAssertionScheduleType.SinceTheLastCheck}>
                        <TextContainer>
                            <Typography.Text strong>Since the previous check</Typography.Text>
                            <Typography.Text type="secondary">
                                We&apos;ll verify that this table has updated since the last scheduled check.
                            </Typography.Text>
                        </TextContainer>
                    </StyledRadio>
                </RadioContainer>
                <RadioContainer>
                    <StyledRadio value={FreshnessAssertionScheduleType.FixedInterval}>
                        <TextContainer>
                            <FixedIntervalScheduleBuilder
                                value={fixedInterval as FixedIntervalSchedule}
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
            </RadioGroup>

            {/* Testing reducing noise */}
            {/* <Typography.Paragraph type="secondary">
                If the table is not updated within the expected time range, this assertion will fail.
            </Typography.Paragraph> */}
        </Form>
    );
};
