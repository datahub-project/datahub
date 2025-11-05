import { Button, colors } from '@components';
import { Form, Input, Select } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { radius } from '@components/theme';

import { SqlEditor } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/field/inputs/SqlEditor';
import {
    onSetValueChange,
    onValueChange,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/field/inputs/utils';
import { getFieldAssertionTypeKey } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/field/utils';
import { AssertionMonitorBuilderState } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';

import { AssertionStdParameterType } from '@types';

// Safe JSON parsing utility to prevent runtime errors
const safeJsonParse = (jsonString: string | undefined, fallback: any = []) => {
    if (!jsonString) return fallback;
    try {
        return JSON.parse(jsonString);
    } catch (error) {
        console.warn('Failed to parse JSON:', jsonString, error);
        return fallback;
    }
};

const StyledSelect = styled(Select)`
    width: 100%;

    .ant-select-selector {
        min-height: 42px !important;
        height: auto !important;
        border: 1px solid ${colors.gray[100]} !important;
        border-radius: ${radius.md} !important;
        background: ${colors.white} !important;
        box-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07) !important;
        padding: 4px 8px !important;

        &:hover {
            box-shadow: 0px 1px 2px 1px rgba(33, 23, 95, 0.07) !important;
        }

        &:focus,
        &:focus-within {
            border-color: ${colors.gray[1800]} !important;
            outline: 1px solid ${colors.violet[200]} !important;
        }
    }

    .ant-select-selection-item {
        margin: 2px 4px 2px 0 !important;
    }

    .ant-select-selection-placeholder {
        margin: 2px 4px 2px 0 !important;
    }
`;

const StyledInput = styled(Input)`
    height: 42px;
    border: 1px solid ${colors.gray[100]};
    border-radius: ${radius.md};
    background: ${colors.white};
    box-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07) !important;

    &:hover {
        box-shadow: 0px 1px 2px 1px rgba(33, 23, 95, 0.07) !important;
    }

    &:focus,
    &:focus-within {
        border-color: ${colors.gray[1800]} !important;
        outline: 1px solid ${colors.violet[200]} !important;
    }
`;

const StyledButton = styled(Button)<{ $active?: boolean }>`
    width: 100%;
    justify-content: center;

    ${(props) =>
        props.$active
            ? `
        background: ${colors.white};
        :hover {
            background: ${colors.white};
        }
    `
            : `
        color: ${colors.gray[500]} !important;
    `}
`;

const TabsWrapper = styled.div`
    display: flex;
    padding: 2px;
    background: ${colors.gray[1500]};
    border-radius: 6px;
    margin-bottom: 8px;
`;

type EditModeInputProps = {
    isSqlAssertion: boolean;
    isSetOperation: boolean;
    isReadOnly: boolean;
    inputType?: string;
    fieldValue: string;
    sqlExpression: string;
    onValuesChange: (newValues: string[]) => void;
    onSingleValueChange: (newValue: string) => void;
    onSqlChange: (newSql: string) => void;
};

const EditModeInput = ({
    isSqlAssertion,
    isSetOperation,
    isReadOnly,
    inputType,
    fieldValue,
    sqlExpression,
    onValuesChange,
    onSingleValueChange,
    onSqlChange,
}: EditModeInputProps) => {
    if (isSqlAssertion) {
        return (
            <SqlEditor
                value={sqlExpression}
                onChange={onSqlChange}
                disabled={isReadOnly}
                height="120px"
                className="query-builder-editor-input"
            />
        );
    }
    if (isSetOperation) {
        return (
            <StyledSelect
                mode="tags"
                placeholder="Press Enter to add values"
                value={safeJsonParse(fieldValue, [])}
                onChange={(newValues) => onValuesChange(newValues as string[])}
                open={false}
                disabled={isReadOnly}
            />
        );
    }
    return (
        <StyledInput
            type={inputType}
            placeholder="Value"
            value={fieldValue || ''}
            onChange={(e) => onSingleValueChange(e.target.value)}
            disabled={isReadOnly}
        />
    );
};

type Props = {
    value: AssertionMonitorBuilderState;
    onChange: (newState: AssertionMonitorBuilderState) => void;
    inputType?: string;
    disabled?: boolean;
    isEditMode?: boolean;
    isSetOperation?: boolean; // Whether this is for a set operation (In/NotIn) or single value
};

export const TabbedInput = ({
    value,
    onChange,
    inputType,
    disabled,
    isEditMode: _isEditMode,
    isSetOperation = false,
}: Props) => {
    const form = Form.useFormInstance();
    const fieldAssertionType = value?.assertion?.fieldAssertion?.type;
    const fieldAssertionKey = fieldAssertionType ? getFieldAssertionTypeKey(fieldAssertionType) : null;
    const fieldValue = fieldAssertionKey
        ? value?.assertion?.fieldAssertion?.[fieldAssertionKey]?.parameters?.value?.value
        : undefined;
    const fieldValueType = fieldAssertionKey
        ? value?.assertion?.fieldAssertion?.[fieldAssertionKey]?.parameters?.value?.type
        : undefined;

    // Initialize state based on current value to prevent UI jittering
    const [activeTab, setActiveTab] = useState<'values' | 'sql'>(() => {
        return fieldValueType === AssertionStdParameterType.Sql ? 'sql' : 'values';
    });
    const [sqlExpression, setSqlExpression] = useState(() => {
        return fieldValueType === AssertionStdParameterType.Sql ? fieldValue || '' : '';
    });

    // Update tab and SQL expression when the value changes
    useEffect(() => {
        if (fieldValueType === AssertionStdParameterType.Sql) {
            setActiveTab('sql');
            setSqlExpression(fieldValue || '');
        } else {
            setActiveTab('values');
        }
    }, [fieldValueType, fieldValue]);

    // Make inputs read-only when disabled, but allow editing in settings mode for field values
    const isReadOnly = Boolean(disabled);

    // Update form field value when the assertion value changes
    useEffect(() => {
        if (fieldValueType === AssertionStdParameterType.Sql) {
            form.setFieldValue('fieldValue', fieldValue);
        } else if (isSetOperation) {
            form.setFieldValue('fieldValue', safeJsonParse(fieldValue, []));
        } else {
            form.setFieldValue('fieldValue', fieldValue);
        }
    }, [form, fieldValue, fieldValueType, isSetOperation]);

    const handleValuesChange = (newValues: string[]) => {
        if (!value) return;

        // This function is only called for set operations (multi-value inputs)
        onSetValueChange(newValues, value, onChange, inputType);
    };

    const handleSingleValueChange = (newValue: string) => {
        if (!value) return;
        onValueChange(newValue, value, onChange);
    };

    const handleSqlChange = (newSql: string) => {
        if (!value || !fieldAssertionType) return;

        setSqlExpression(newSql);

        // Update the assertion state with SQL expression
        const currentFieldAssertionKey = getFieldAssertionTypeKey(fieldAssertionType);
        onChange({
            ...value,
            assertion: {
                ...value.assertion,
                fieldAssertion: {
                    ...value.assertion?.fieldAssertion,
                    [currentFieldAssertionKey]: {
                        ...value.assertion?.fieldAssertion?.[currentFieldAssertionKey],
                        parameters: {
                            ...value.assertion?.fieldAssertion?.[currentFieldAssertionKey]?.parameters,
                            value: {
                                value: newSql,
                                type: AssertionStdParameterType.Sql,
                            },
                        },
                    },
                },
            },
        });
    };

    const handleTabChange = (key: string) => {
        if (!value || !fieldAssertionType || isReadOnly) return;

        const newTab = key as 'values' | 'sql';
        setActiveTab(newTab);

        const currentFieldAssertionKey = getFieldAssertionTypeKey(fieldAssertionType);

        // Clear the other input when switching tabs
        if (newTab === 'values') {
            // Clear SQL expression and set appropriate type for values
            setSqlExpression('');
            const defaultValue = isSetOperation ? '[]' : '';
            const defaultType = isSetOperation ? AssertionStdParameterType.Set : AssertionStdParameterType.String;

            onChange({
                ...value,
                assertion: {
                    ...value.assertion,
                    fieldAssertion: {
                        ...value.assertion?.fieldAssertion,
                        [currentFieldAssertionKey]: {
                            ...value.assertion?.fieldAssertion?.[currentFieldAssertionKey],
                            parameters: {
                                ...value.assertion?.fieldAssertion?.[currentFieldAssertionKey]?.parameters,
                                value: {
                                    value: defaultValue,
                                    type: defaultType,
                                },
                            },
                        },
                    },
                },
            });
        } else {
            // Clear values when switching to SQL and set SQL type
            onChange({
                ...value,
                assertion: {
                    ...value.assertion,
                    fieldAssertion: {
                        ...value.assertion?.fieldAssertion,
                        [currentFieldAssertionKey]: {
                            ...value.assertion?.fieldAssertion?.[currentFieldAssertionKey],
                            parameters: {
                                ...value.assertion?.fieldAssertion?.[currentFieldAssertionKey]?.parameters,
                                value: {
                                    value: '',
                                    type: AssertionStdParameterType.Sql,
                                },
                            },
                        },
                    },
                },
            });
        }
    };

    // Determine if this should show SQL input based on current assertion type
    const isSqlAssertion = fieldValueType === AssertionStdParameterType.Sql;

    // Show tabs in both create and edit flows
    const showTabs = isSetOperation;

    // Only require a value when not using SQL input. When using SQL, show a hint instead of a required error.
    const formRules = isSqlAssertion ? [] : [{ required: true, message: 'Required' }];

    return (
        <Form.Item
            name="fieldValue"
            rules={formRules}
            help={isSetOperation && isSqlAssertion ? 'SQL query should return a single column of values.' : undefined}
        >
            {showTabs ? (
                <div>
                    <TabsWrapper>
                        <StyledButton
                            $active={activeTab === 'values'}
                            onClick={() => handleTabChange('values')}
                            variant="text"
                            isDisabled={isReadOnly}
                        >
                            Values
                        </StyledButton>
                        <StyledButton
                            $active={activeTab === 'sql'}
                            onClick={() => handleTabChange('sql')}
                            variant="text"
                            isDisabled={isReadOnly}
                        >
                            SQL
                        </StyledButton>
                    </TabsWrapper>
                    <div>
                        {activeTab === 'values' && isSetOperation && (
                            <StyledSelect
                                mode="tags"
                                placeholder="Press Enter to add values"
                                value={safeJsonParse(fieldValue, [])}
                                onChange={(newValues) => handleValuesChange(newValues as string[])}
                                open={false}
                                disabled={isReadOnly}
                            />
                        )}
                        {activeTab === 'values' && !isSetOperation && (
                            <StyledInput
                                type={inputType}
                                placeholder="Value"
                                value={fieldValue || ''}
                                onChange={(e) => handleSingleValueChange(e.target.value)}
                                disabled={isReadOnly}
                            />
                        )}
                        {activeTab === 'sql' && (
                            <SqlEditor
                                value={sqlExpression}
                                onChange={handleSqlChange}
                                disabled={isReadOnly}
                                height="120px"
                                className="query-builder-editor-input"
                            />
                        )}
                    </div>
                </div>
            ) : (
                // In edit mode, show only the relevant input based on current type
                <EditModeInput
                    isSqlAssertion={isSqlAssertion}
                    isSetOperation={isSetOperation}
                    isReadOnly={isReadOnly}
                    inputType={inputType}
                    fieldValue={fieldValue || ''}
                    sqlExpression={sqlExpression}
                    onValuesChange={handleValuesChange}
                    onSingleValueChange={handleSingleValueChange}
                    onSqlChange={handleSqlChange}
                />
            )}
        </Form.Item>
    );
};
