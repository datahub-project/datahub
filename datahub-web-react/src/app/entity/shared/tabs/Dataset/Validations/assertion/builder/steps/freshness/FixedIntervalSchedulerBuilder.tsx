import React from 'react';
import styled from 'styled-components';
import { InputNumber, Select, Typography } from 'antd';
import { DateInterval, FixedIntervalSchedule } from '../../../../../../../../../../types.generated';
import {
    DEFAULT_ASSERTION_EVALUATION_INTERVAL_MULTIPLE,
    DEFAULT_ASSERTION_EVALUATION_INTERVAL_UNIT,
} from '../../constants';

const Title = styled(Typography.Title)`
    && {
        margin-bottom: 16px;
    }
`;

const Form = styled.div``;

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 12px;
`;

const ScheduleDefinition = styled.div`
    display: flex;
    align-items: center;
    justify-content: left;
`;

const ScheduleText = styled(Typography.Text)`
    margin-right: 8px;
`;

const MultipleInput = styled(InputNumber)`
    margin-right: 8px;
`;

const Description = styled(Typography.Paragraph)`
    && {
        padding: 0px;
        margin: 0px;
    }
`;

type Props = {
    value?: FixedIntervalSchedule | null;
    onChange: (newSchedule: FixedIntervalSchedule) => void;
};

/**
 * Builder used to construct a Fixed Interval suitable for Assertion Evaluation.
 */
export const FixedIntervalScheduleBuilder = ({ value, onChange }: Props) => {
    const unit = value?.unit || DEFAULT_ASSERTION_EVALUATION_INTERVAL_UNIT;
    const multiple = value?.multiple || DEFAULT_ASSERTION_EVALUATION_INTERVAL_MULTIPLE;

    const updateUnit = (newUnit: DateInterval) => {
        onChange({
            unit: newUnit,
            multiple,
        });
    };

    const updateMultiple = (newMultiple: number | string | null) => {
        if (newMultiple) {
            onChange({
                unit,
                multiple: newMultiple as number,
            });
        }
    };

    return (
        <Form>
            <Title level={5}>Expected Change Interval</Title>
            <Section>
                <ScheduleDefinition>
                    <ScheduleText type="secondary">Every</ScheduleText>
                    <MultipleInput min={1} max={500} value={multiple} onChange={updateMultiple} />
                    <Select value={unit} onChange={updateUnit}>
                        <Select.Option value={DateInterval.Minute}>minutes</Select.Option>
                        <Select.Option value={DateInterval.Hour}>hours</Select.Option>
                        <Select.Option value={DateInterval.Day}>days</Select.Option>
                    </Select>
                </ScheduleDefinition>
            </Section>
            <Section>
                <Description type="secondary">
                    Assertion will fail if this dataset has not changed in{' '}
                    <b>
                        {multiple} {unit.toLocaleLowerCase()}s
                    </b>
                    .
                </Description>
            </Section>
        </Form>
    );
};
