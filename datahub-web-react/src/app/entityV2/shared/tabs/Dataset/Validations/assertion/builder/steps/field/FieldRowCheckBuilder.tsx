import React from 'react';
import Typography from 'antd/lib/typography';
import styled from 'styled-components';
import { Radio, RadioChangeEvent, Tooltip } from 'antd';
import { AssertionMonitorBuilderState } from '../../types';
import { DatasetFieldAssertionSourceType } from '../../../../../../../../../../types.generated';
import { ANTD_GRAY } from '../../../../../../../constants';
import { FieldChangedRowsBuilder } from './FieldChangedRowsBuilder';
import { getDatasetProfileDisabledMessage } from './utils';
import { useConnectionForEntityExists } from '../../../../acrylUtils';

const Section = styled.div`
    margin: 16px 0 24px;
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
    value: AssertionMonitorBuilderState;
    onChange: (newState: AssertionMonitorBuilderState) => void;
    disabled?: boolean;
};

export const FieldRowCheckBuilder = ({ value, onChange, disabled }: Props) => {
    const connectionForEntityExists = useConnectionForEntityExists(value.entityUrn as string);
    const sourceType = value.parameters?.datasetFieldParameters?.sourceType;
    const disabledMessage =
        getDatasetProfileDisabledMessage(value.platformUrn as string, true, connectionForEntityExists) ||
        'Not supported when using DataHub Dataset Profiles as the data source.';
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
        <Section>
            <Typography.Title level={5}>Evaluate the condition for</Typography.Title>
            <RadioGroup
                value={
                    sourceType === DatasetFieldAssertionSourceType.DatahubDatasetProfile
                        ? DatasetFieldAssertionSourceType.AllRowsQuery
                        : sourceType
                }
                onChange={(e: RadioChangeEvent) => updateSourceType(e.target.value)}
                disabled={disabled}
            >
                <RadioContainer>
                    <StyledRadio value={DatasetFieldAssertionSourceType.AllRowsQuery}>
                        <TextContainer>
                            <Typography.Text strong>All table rows</Typography.Text>
                            <Typography.Text type="secondary">
                                Each time we run the check, we’ll evaluate the condition using all rows in the table.
                                This may not be desirable for large tables.
                            </Typography.Text>
                        </TextContainer>
                    </StyledRadio>
                </RadioContainer>
                <RadioContainer>
                    <StyledRadio
                        value={DatasetFieldAssertionSourceType.ChangedRowsQuery}
                        disabled={sourceType === DatasetFieldAssertionSourceType.DatahubDatasetProfile}
                    >
                        <Tooltip
                            placement="bottom"
                            title={
                                sourceType === DatasetFieldAssertionSourceType.DatahubDatasetProfile
                                    ? disabledMessage
                                    : undefined
                            }
                        >
                            <TextContainer>
                                <Typography.Text strong>Only rows that have changed</Typography.Text>
                                <Typography.Text type="secondary">
                                    Each time we run the check, we’ll evaluate the condition using only the rows that
                                    have changed since the previous check.
                                </Typography.Text>
                                <FieldChangedRowsBuilder value={value} onChange={onChange} disabled={disabled} />
                            </TextContainer>
                        </Tooltip>
                    </StyledRadio>
                </RadioContainer>
            </RadioGroup>
        </Section>
    );
};
