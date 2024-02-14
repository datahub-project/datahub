import React from 'react';
import Typography from 'antd/lib/typography';
import styled from 'styled-components';
import { Select, Tooltip } from 'antd';
import { AssertionMonitorBuilderState } from '../../types';
import { DatasetFieldAssertionSourceType } from '../../../../../../../../../../types.generated';
import {
    getDatasetProfileDisabledMessage,
    getFieldMetricSourceTypeOptions,
    getInvalidMetricMessage,
    getSelectedFieldMetricTypeOption,
} from './utils';
import { useConnectionForEntityExists } from '../../../../acrylUtils';

const StyledSelect = styled(Select)`
    width: 340px;
`;

const SelectOptionContent = styled.div<{ disabled: boolean }>`
    opacity: ${(props) => (props.disabled ? 0.5 : 1)};
`;

const OptionDescription = styled(Typography.Paragraph)`
    && {
        margin-bottom: 4px;
        overflow-wrap: break-word;
        white-space: normal;
    }
`;

type Props = {
    value: AssertionMonitorBuilderState;
    onChange: (newState: AssertionMonitorBuilderState) => void;
    disabled?: boolean;
};

export const FieldMetricSourceBuilder = ({ value, onChange, disabled }: Props) => {
    const connectionForEntityExists = useConnectionForEntityExists(value.entityUrn as string);
    const fieldType = value.assertion?.fieldAssertion?.fieldMetricAssertion?.field?.type;
    const metricType = value.assertion?.fieldAssertion?.fieldMetricAssertion?.metric;
    const sourceType = value.parameters?.datasetFieldParameters?.sourceType;
    const selectedMetricConfig = getSelectedFieldMetricTypeOption(fieldType, metricType);
    const sourceOptions = getFieldMetricSourceTypeOptions();
    const updateSourceType = (newSourceType: DatasetFieldAssertionSourceType) => {
        onChange({
            ...value,
            parameters: {
                ...value.parameters,
                datasetFieldParameters: {
                    ...value.parameters?.datasetFieldParameters,
                    sourceType: newSourceType,
                },
            },
        });
    };

    return (
        <div>
            <Typography.Title level={5}>Change Source</Typography.Title>
            <Typography.Paragraph type="secondary">
                Select the mechanism used to determine how the metric is calculated.
            </Typography.Paragraph>
            <StyledSelect
                value={
                    sourceType === DatasetFieldAssertionSourceType.DatahubDatasetProfile
                        ? DatasetFieldAssertionSourceType.DatahubDatasetProfile
                        : DatasetFieldAssertionSourceType.AllRowsQuery
                }
                onChange={(sourceOption) => updateSourceType(sourceOption as DatasetFieldAssertionSourceType)}
                disabled={disabled}
            >
                {sourceOptions.map((option) => {
                    const invalidConnectionMessage = getDatasetProfileDisabledMessage(
                        value.platformUrn as string,
                        option.requiresConnection,
                        connectionForEntityExists,
                    );
                    const invalidMetricMessage = getInvalidMetricMessage(
                        option.requiresConnection,
                        selectedMetricConfig?.requiresConnection,
                    );
                    const disabledMessage = invalidMetricMessage || invalidConnectionMessage;

                    return (
                        <Select.Option value={option.value} key={option.value} disabled={!!disabledMessage}>
                            <Tooltip placement="right" title={disabledMessage || undefined}>
                                <SelectOptionContent disabled={!!disabledMessage}>
                                    <Typography.Text>{option.label}</Typography.Text>
                                    <OptionDescription type="secondary">{option.description}</OptionDescription>
                                </SelectOptionContent>
                            </Tooltip>
                        </Select.Option>
                    );
                })}
            </StyledSelect>
        </div>
    );
};
