import React from 'react';
import Typography from 'antd/lib/typography';
import styled from 'styled-components';
import { Select } from 'antd';
import { AssertionMonitorBuilderState } from '../../types';
import {
    SQL_OPERATION_OPTIONS,
    SqlOperationOptionEnum,
    getDefaultOperationOption,
    getSqlOperationOptions,
} from './utils';
import { SqlParametersBuilder } from './SqlParametersBuilder';
import { AssertionStdOperator, SqlAssertionType } from '../../../../../../../../../../types.generated';

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
    const defaultOption = getDefaultOperationOption(
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
            <Typography.Title level={5}>Fail if resulting value</Typography.Title>
            <StyledSelect
                defaultValue={SQL_OPERATION_OPTIONS[defaultOption]}
                options={options}
                onChange={(newOption) => updateOperationOption(newOption as SqlOperationOptionEnum)}
                disabled={disabled}
            />
            <SqlParametersBuilder value={value} onChange={onChange} disabled={disabled} />
        </Section>
    );
};
