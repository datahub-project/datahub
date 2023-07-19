import React from 'react';
import styled from 'styled-components';
import { Radio, Typography } from 'antd';
import { InfoCircleOutlined } from '@ant-design/icons';
import {
    DatasetFreshnessAssertionParameters,
    DatasetFreshnessSourceType,
    FreshnessFieldSpec,
} from '../../../../../../../../../../types.generated';
import { FieldValueSourceBuilder } from './FieldValueSourceBuilder';
import { useGetDatasetSchemaQuery } from '../../../../../../../../../../graphql/dataset.generated';
import { ANTD_GRAY } from '../../../../../../../constants';
import { SourceOption, getSourceOption, getSourceOptions } from '../../utils';

const Form = styled.div``;

const SourceDescription = styled.div`
    margin-top: 12px;
    margin-bottom: 12px;
`;

const StyledInfoCircleOutlined = styled(InfoCircleOutlined)`
    color: ${ANTD_GRAY[6]};
    margin-right: 4px;
`;

type Props = {
    entityUrn: string;
    platformUrn: string;
    value?: DatasetFreshnessAssertionParameters | null;
    onChange: (newParams: DatasetFreshnessAssertionParameters) => void;
};

/**
 * Builder used to configure the Source Type of Dataset Freshness assertion evaluations.
 * This represents the signal that is used as input when determining whether an Freshness has been
 * missed or not.
 *
 * If demand exists, we can extend this support customizing the audit-log based Freshness assertions to include;:
 *
 *     - Operation types (monitor for inserts, updates, deletes, create tables specifically)
 *     - Row counts (min number of rows that change for a given operation)
 *     - User who performed the update
 *
 * For applicable sources
 */
export const DatasetFreshnessSourceBuilder = ({ entityUrn, platformUrn, value, onChange }: Props) => {
    const sourceType = value?.sourceType || DatasetFreshnessSourceType.AuditLog;
    const field = value?.field;
    const fieldKind = field?.kind;

    const { data } = useGetDatasetSchemaQuery({
        variables: {
            urn: entityUrn,
        },
        fetchPolicy: 'cache-first',
    });

    const sourceOptions = getSourceOptions(platformUrn);
    const selectedSourceOption = getSourceOption(sourceType, fieldKind);

    /**
     * Extract the schema fields eligible for selection. These must be timestamp type fields.
     */
    const eligibleFields =
        data?.dataset?.schemaMetadata?.fields?.filter(
            (f) => selectedSourceOption?.field?.dataTypes?.has(f.type) && f.nativeDataType,
        ) || [];
    const eligibleFieldSpecs = eligibleFields.map((f) => ({
        path: f.fieldPath,
        type: f.type,
        nativeType: f.nativeDataType as string,
    }));

    const updateSourceType = (newSourceOption: SourceOption) => {
        const newField = newSourceOption.field
            ? {
                  kind: newSourceOption.field.kind,
              }
            : undefined;
        onChange({
            ...value,
            sourceType: newSourceOption.type,
            field: newField as FreshnessFieldSpec,
        });
    };

    const updateFieldSpec = (newSpec: Partial<FreshnessFieldSpec>) => {
        onChange({
            ...value,
            field: {
                ...field,
                ...newSpec,
            },
        } as any);
    };

    return (
        <Form>
            <Typography.Title level={5}>Change Source</Typography.Title>
            <Typography.Paragraph type="secondary">
                Select the mechanism used to determine whether a change has been made to this dataset.
            </Typography.Paragraph>
            <Radio.Group value={sourceType} onChange={(e) => updateSourceType(e.target.value)}>
                {sourceOptions.map((option) => (
                    <Radio.Button value={option}>{option.name}</Radio.Button>
                ))}
            </Radio.Group>
            <SourceDescription>
                <StyledInfoCircleOutlined />
                {selectedSourceOption.description}
            </SourceDescription>
            {sourceType === DatasetFreshnessSourceType.FieldValue && (
                <FieldValueSourceBuilder fields={eligibleFieldSpecs} value={field} onChange={updateFieldSpec} />
            )}
            {selectedSourceOption.secondaryDescription && (
                <Typography.Text type="secondary">{selectedSourceOption.secondaryDescription}</Typography.Text>
            )}
        </Form>
    );
};
