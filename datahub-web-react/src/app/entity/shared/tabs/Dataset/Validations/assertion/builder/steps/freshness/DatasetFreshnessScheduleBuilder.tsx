import React from 'react';
import styled from 'styled-components';
import { Radio, RadioChangeEvent, Typography } from 'antd';
import {
    FixedIntervalSchedule,
    FreshnessAssertionSchedule,
    FreshnessAssertionScheduleType,
} from '../../../../../../../../../../types.generated';
import { ANTD_GRAY } from '../../../../../../../constants';
import { FixedIntervalScheduleBuilder } from './FixedIntervalSchedulerBuilder';

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
    background-color: ${ANTD_GRAY[2]};
    border: 1px solid ${ANTD_GRAY[5]};
    border-radius: 8px;
    padding: 8px 16px;
    height: 80px;
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
            <Title level={5}>Fail if this table has not changed</Title>
            <RadioGroup
                value={scheduleType}
                onChange={(e: RadioChangeEvent) => handleScheduleTypeChange(e.target.value)}
                disabled={disabled}
            >
                <RadioContainer>
                    <StyledRadio value={FreshnessAssertionScheduleType.Cron}>
                        <TextContainer>
                            <Typography.Text strong>Since the previous check</Typography.Text>
                            <Typography.Text type="secondary">
                                We&apos;ll verify that this table has changed since the last scheduled check.
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
                                    We&apos;ll verify that this table has changed within {fixedInterval?.multiple}{' '}
                                    {fixedInterval?.unit.toLocaleLowerCase()}s of the scheduled check.
                                </Typography.Text>
                            )}
                        </TextContainer>
                    </StyledRadio>
                </RadioContainer>
            </RadioGroup>
            <Typography.Paragraph type="secondary">
                If the table is not updated within the expected time range, this assertion will fail.
            </Typography.Paragraph>
        </Form>
    );
};
