import { Select } from 'antd';
import Typography from 'antd/lib/typography';
import React from 'react';
import styled from 'styled-components';

import {
    AI_INFERRED_ASSERTION_DEFAULT_SCHEDULE_CRON,
    AI_INFERRED_ASSERTION_DEFAULT_SCHEDULE_TIMEZONE,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/constants';
import { SqlParametersBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/sql/SqlParametersBuilder';
import {
    SQL_OPERATION_OPTIONS,
    SqlOperationOptionEnum,
    getOperationOption,
    getSqlOperationOptions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/sql/utils';
import { AssertionMonitorBuilderState } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';
import { nullsToUndefined } from '@src/app/entityV2/shared/utils';

// no direct type imports needed from '@types' in this file

const Section = styled.div`
    margin: 16px 0 24px;
`;

const Row = styled.div`
    display: flex;
    gap: 16px;
    width: 100%;
`;

const StyledSelect = styled(Select)`
    flex: 2;
`;

type Props = {
    value: AssertionMonitorBuilderState;
    onChange: (newState: AssertionMonitorBuilderState) => void;
    disabled?: boolean;
};

export const SqlEvaluationBuilder = ({ value, onChange, disabled }: Props) => {
    const options = getSqlOperationOptions();
    const optionValue = getOperationOption(
        value.assertion?.sqlAssertion?.type,
        value.assertion?.sqlAssertion?.operator,
    );

    const hasParameters = Boolean(optionValue && optionValue !== SqlOperationOptionEnum.AI_INFERRED);

    const updateOperationOption = (option: SqlOperationOptionEnum) => {
        const operation = SQL_OPERATION_OPTIONS[option];
        const isAiInferred = option === SqlOperationOptionEnum.AI_INFERRED;

        onChange({
            ...value,
            schedule: isAiInferred
                ? {
                      cron: AI_INFERRED_ASSERTION_DEFAULT_SCHEDULE_CRON,
                      timezone: AI_INFERRED_ASSERTION_DEFAULT_SCHEDULE_TIMEZONE,
                  }
                : value.schedule,
            assertion: {
                ...value.assertion,
                sqlAssertion: {
                    ...value.assertion?.sqlAssertion,
                    type: operation.type,
                    operator: operation.operator,
                    changeType: operation.changeType,
                    parameters: isAiInferred
                        ? {}
                        : {
                              ...value.assertion?.sqlAssertion?.parameters,
                              ...nullsToUndefined(operation.parameters),
                          },
                },
            },
        });
    };

    return (
        <Section>
            <Typography.Title level={5}>Pass if resulting value</Typography.Title>
            <Row>
                <StyledSelect
                    value={optionValue ? SQL_OPERATION_OPTIONS[optionValue] : undefined}
                    placeholder="Select condition"
                    options={options}
                    onChange={(newOption) => updateOperationOption(newOption as SqlOperationOptionEnum)}
                    disabled={disabled}
                />
                {hasParameters && <SqlParametersBuilder value={value} onChange={onChange} disabled={disabled} />}
            </Row>
        </Section>
    );
};
