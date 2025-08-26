import { Select } from 'antd';
import Typography from 'antd/lib/typography';
import React from 'react';
import styled from 'styled-components';

import { SqlParametersBuilder } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/sql/SqlParametersBuilder';
import {
    SQL_OPERATION_OPTIONS,
    SqlOperationOptionEnum,
    getOperationOption,
    getSqlOperationOptions,
} from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/sql/utils';
import { AssertionMonitorBuilderState } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/types';

import { AssertionStdOperator, SqlAssertionType } from '@types';

const Section = styled.div`
    margin: 16px 0 24px;
`;

const StyledSelect = styled(Select)`
    width: 250px;
    margin-right: 16px;
`;

type Props = {
    value: AssertionMonitorBuilderState;
    onChange: (newState: AssertionMonitorBuilderState) => void;
    disabled?: boolean;
};

export const SqlEvaluationBuilder = ({ value, onChange, disabled }: Props) => {
    const options = getSqlOperationOptions();
    const optionValue = getOperationOption(
        value.assertion?.sqlAssertion?.type as SqlAssertionType,
        value.assertion?.sqlAssertion?.operator as AssertionStdOperator,
    );

    const updateOperationOption = (option: SqlOperationOptionEnum) => {
        const operation = SQL_OPERATION_OPTIONS[option];
        onChange({
            ...value,
            assertion: {
                ...value.assertion,
                sqlAssertion: {
                    ...value.assertion?.sqlAssertion,
                    type: operation.type,
                    operator: operation.operator,
                    changeType: operation.changeType,
                    parameters: {
                        ...value.assertion?.sqlAssertion?.parameters,
                        ...operation.parameters,
                    },
                },
            },
        });
    };

    return (
        <Section>
            <Typography.Title level={5}>Pass if resulting value</Typography.Title>
            <StyledSelect
                value={SQL_OPERATION_OPTIONS[optionValue]}
                options={options}
                onChange={(newOption) => updateOperationOption(newOption as SqlOperationOptionEnum)}
                disabled={disabled}
            />
            <SqlParametersBuilder value={value} onChange={onChange} disabled={disabled} />
        </Section>
    );
};
