import React from 'react';
import styled from 'styled-components';
import { InputNumber, Select, Typography } from 'antd';
import { DateInterval, FixedIntervalSchedule } from '../../../../../../../../../../types.generated';
import {
    DEFAULT_ASSERTION_EVALUATION_INTERVAL_MULTIPLE,
    DEFAULT_ASSERTION_EVALUATION_INTERVAL_UNIT,
} from '../../constants';
import { StopPropagation } from '../../../../../../../../../shared/StopPropagation';

const Form = styled.div``;

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

type Props = {
    value?: FixedIntervalSchedule | null;
    onChange: (newSchedule: FixedIntervalSchedule) => void;
    disabled?: boolean;
    stopPropagation?: boolean;
};

/**
 * Builder used to construct a Fixed Interval suitable for Assertion Evaluation.
 */
export const FixedIntervalScheduleBuilder = ({ value, onChange, disabled, stopPropagation }: Props) => {
    const unit = value?.unit || DEFAULT_ASSERTION_EVALUATION_INTERVAL_UNIT;
    const multiple = value?.multiple || DEFAULT_ASSERTION_EVALUATION_INTERVAL_MULTIPLE;
    const MaybeStopPropagation = stopPropagation ? StopPropagation : React.Fragment;

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
            <ScheduleDefinition>
                <ScheduleText strong>In the past</ScheduleText>
                <MultipleInput min={1} max={500} value={multiple} onChange={updateMultiple} disabled={disabled} />
                <MaybeStopPropagation>
                    <Select value={unit} onChange={updateUnit} disabled={disabled}>
                        <Select.Option value={DateInterval.Minute}>minutes</Select.Option>
                        <Select.Option value={DateInterval.Hour}>hours</Select.Option>
                        <Select.Option value={DateInterval.Day}>days</Select.Option>
                    </Select>
                </MaybeStopPropagation>
            </ScheduleDefinition>
        </Form>
    );
};
