import { InfoCircleOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { Switch } from 'antd';
import Typography from 'antd/lib/typography';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { AssertionMonitorBuilderState } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/types';

import { AssertionStdOperator } from '@types';

const Section = styled.div`
    margin-top: 16px;
`;

const TitleSection = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 8px;
`;

const Title = styled(Typography.Paragraph)`
    && {
        margin: 0;
    }
`;

type Props = {
    value: AssertionMonitorBuilderState;
    onChange: (newState: AssertionMonitorBuilderState) => void;
    disabled?: boolean;
};

export const FieldNullCheckBuilder = ({ value, onChange, disabled }: Props) => {
    const operator = value.assertion?.fieldAssertion?.fieldValuesAssertion?.operator;
    const showForm = operator && ![AssertionStdOperator.Null, AssertionStdOperator.NotNull].includes(operator);
    const handleToggle = (newValue: boolean) => {
        onChange({
            ...value,
            assertion: {
                ...value.assertion,
                fieldAssertion: {
                    ...value.assertion?.fieldAssertion,
                    fieldValuesAssertion: {
                        ...value.assertion?.fieldAssertion?.fieldValuesAssertion,
                        excludeNulls: newValue,
                    },
                },
            },
        });
    };

    return showForm ? (
        <Section>
            <TitleSection>
                <Title strong>Allow nulls?</Title>
                <Tooltip
                    color={ANTD_GRAY[9]}
                    placement="right"
                    title="If disabled, any null column values will be reported as a failure when evaluating this assertion."
                >
                    <InfoCircleOutlined />
                </Tooltip>
            </TitleSection>
            <Switch
                checked={value.assertion?.fieldAssertion?.fieldValuesAssertion?.excludeNulls ?? true}
                onChange={handleToggle}
                disabled={disabled}
            />
        </Section>
    ) : null;
};
