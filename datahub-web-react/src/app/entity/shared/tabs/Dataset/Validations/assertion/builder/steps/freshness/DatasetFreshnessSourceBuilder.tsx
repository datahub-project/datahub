import React from 'react';
import styled from 'styled-components';
import { Typography, Select } from 'antd';
import { InfoCircleOutlined } from '@ant-design/icons';
import {
    DatasetFreshnessAssertionParameters,
    DatasetFreshnessSourceType,
    FreshnessFieldSpec,
} from '../../../../../../../../../../types.generated';
import { FieldValueSourceBuilder } from './FieldValueSourceBuilder';
import { useGetDatasetSchemaQuery } from '../../../../../../../../../../graphql/dataset.generated';
import { ANTD_GRAY } from '../../../../../../../constants';
import {
    getFreshnessSourceOption,
    getFreshnessSourceOptions,
    getFreshnessSourceOptionPlatformDescription,
    getDefaultFreshnessSourceOption,
} from '../../utils';

const Form = styled.div``;

const SourceDescription = styled.div`
    margin-top: 12px;
    margin-bottom: 12px;
    display: flex;
    align-items: center;
    justify-content: left;
    border: 1px solid ${ANTD_GRAY[5]};
    padding: 12px;
    background-color: ${ANTD_GRAY[3]};
    border-radius: 8px;
    margin-bottom: 20px;
`;

const StyledInfoCircleOutlined = styled(InfoCircleOutlined)`
    color: ${ANTD_GRAY[7]};
    margin-right: 4px;
`;

const StyledSelect = styled(Select)`
    max-width: 340px;
`;

const PlatformDescription = styled.div`
    margin-left: 8px;
`;

const SelectColumnDescription = styled.div`
    margin-bottom: 12px;
`;

const SourceOptionSelectDescription = styled(Typography.Paragraph)`
    && {
        word-wrap: break-word;
        white-space: break-spaces;
    }
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
    const sourceType = value?.sourceType || getDefaultFreshnessSourceOption(platformUrn);
    const field = value?.field;
    const fieldKind = field?.kind;

    const { data } = useGetDatasetSchemaQuery({
        variables: {
            urn: entityUrn,
        },
        fetchPolicy: 'cache-first',
    });

    const sourceOptions = getFreshnessSourceOptions(platformUrn);
    const selectedSourceOption = getFreshnessSourceOption(sourceType, fieldKind);
    const platformDescription = getFreshnessSourceOptionPlatformDescription(platformUrn, sourceType);

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

    const updateSourceType = (newSourceType: string) => {
        const newSourceOption = sourceOptions.find((option) => option.name === newSourceType);
        if (newSourceOption) {
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
        }
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
            <StyledSelect
                value={selectedSourceOption.name}
                onChange={(sourceOption) => updateSourceType(sourceOption as string)}
            >
                {sourceOptions.map((option) => (
                    // In order to access the entire object on change, we use the object reference as the value and then override the default checked behavior
                    <Select.Option value={option.name} key={option.name}>
                        <Typography.Text>{option.name}</Typography.Text>
                        <SourceOptionSelectDescription type="secondary">
                            {selectedSourceOption.description}
                        </SourceOptionSelectDescription>
                    </Select.Option>
                ))}
            </StyledSelect>
            <SourceDescription>
                <StyledInfoCircleOutlined />
                <PlatformDescription>{platformDescription}</PlatformDescription>
            </SourceDescription>
            {selectedSourceOption.secondaryDescription && (
                <>
                    <Typography.Title level={5}>Select Column</Typography.Title>
                    <SelectColumnDescription>
                        <Typography.Text type="secondary">{selectedSourceOption.secondaryDescription}</Typography.Text>
                    </SelectColumnDescription>
                </>
            )}
            {sourceType === DatasetFreshnessSourceType.FieldValue && (
                <FieldValueSourceBuilder fields={eligibleFieldSpecs} value={field} onChange={updateFieldSpec} />
            )}
        </Form>
    );
};
