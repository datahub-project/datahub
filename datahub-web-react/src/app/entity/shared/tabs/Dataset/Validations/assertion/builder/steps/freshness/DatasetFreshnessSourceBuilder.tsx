import React, { useEffect } from 'react';
import styled from 'styled-components';
import { Typography, Select } from 'antd';
import { InfoCircleOutlined } from '@ant-design/icons';
import useFormInstance from 'antd/lib/form/hooks/useFormInstance';
import {
    DatasetFreshnessAssertionParameters,
    DatasetFreshnessSourceType,
    FreshnessAssertionScheduleType,
    FreshnessFieldSpec,
    SchemaField,
} from '../../../../../../../../../../types.generated';
import { useGetDatasetSchemaQuery } from '../../../../../../../../../../graphql/dataset.generated';
import { ANTD_GRAY } from '../../../../../../../constants';
import {
    getFreshnessSourceOption,
    getFreshnessSourceOptions,
    getFreshnessSourceOptionPlatformDescription,
    getDefaultFreshnessSourceOption,
    isStructField,
} from '../../utils';
import { useChangeSourceOptionIf } from '../../hooks';
import { AssertionDatasetFieldBuilder } from '../AssertionDatasetFieldBuilder';
import { useConnectionForEntityExists } from '../../../../acrylUtils';

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
    width: 340px;
`;

const SelectOptionContent = styled.div<{ disabled: boolean }>`
    opacity: ${(props) => (props.disabled ? 0.5 : 1)};
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
    scheduleType: FreshnessAssertionScheduleType;
    value?: DatasetFreshnessAssertionParameters | null;
    onChange: (newParams: DatasetFreshnessAssertionParameters) => void;
    disabled?: boolean;
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
export const DatasetFreshnessSourceBuilder = ({
    entityUrn,
    platformUrn,
    scheduleType,
    value,
    onChange,
    disabled,
}: Props) => {
    const form = useFormInstance();
    const connectionForEntityExists = useConnectionForEntityExists(entityUrn);
    const defaultSourceType = getDefaultFreshnessSourceOption(platformUrn, connectionForEntityExists);
    const sourceType = value?.sourceType || defaultSourceType;
    const field = value?.field;
    const fieldKind = field?.kind;
    const fieldPath = field?.path;

    const { data } = useGetDatasetSchemaQuery({
        variables: {
            urn: entityUrn,
        },
        fetchPolicy: 'cache-first',
    });

    const sourceOptions = getFreshnessSourceOptions(platformUrn, connectionForEntityExists);
    const selectedSourceOption = getFreshnessSourceOption(sourceType, fieldKind);
    const platformDescription = getFreshnessSourceOptionPlatformDescription(platformUrn, sourceType);

    /**
     * Extract the schema fields eligible for selection. These must be timestamp type fields.
     */
    const eligibleFields =
        data?.dataset?.schemaMetadata?.fields?.filter(
            (f) =>
                selectedSourceOption?.field?.dataTypes?.has(f.type) &&
                f.nativeDataType &&
                !isStructField(f as SchemaField),
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

    const updateFieldSpec = (newPath: string) => {
        const newSpec = eligibleFieldSpecs.filter((spec) => spec.path === newPath)[0];
        onChange({
            ...value,
            field: {
                ...field,
                ...newSpec,
            },
        } as any);
    };

    // If the selected source option is not allowed for the current schedule type, then change the source type to the default
    useChangeSourceOptionIf(!selectedSourceOption.allowedScheduleTypes.includes(scheduleType), {
        sourceType: defaultSourceType,
        updateSourceType,
    });

    useEffect(() => {
        form.setFieldValue('column', fieldPath);
    }, [form, fieldPath]);

    return (
        <Form>
            <Typography.Title level={5}>Change Source</Typography.Title>
            <Typography.Paragraph type="secondary">
                Select the mechanism used to determine whether a change has been made to this dataset.
            </Typography.Paragraph>
            <StyledSelect
                value={selectedSourceOption.name}
                onChange={(sourceOption) => updateSourceType(sourceOption as string)}
                disabled={disabled}
            >
                {sourceOptions.map((option) => {
                    const isDisabled = !option.allowedScheduleTypes.includes(scheduleType);
                    return (
                        <Select.Option value={option.name} key={option.name} disabled={isDisabled}>
                            <SelectOptionContent disabled={isDisabled}>
                                <Typography.Text>{option.name}</Typography.Text>
                                <SourceOptionSelectDescription type="secondary">
                                    {option.description}
                                </SourceOptionSelectDescription>
                            </SelectOptionContent>
                        </Select.Option>
                    );
                })}
            </StyledSelect>
            {platformDescription && (
                <SourceDescription>
                    <StyledInfoCircleOutlined />
                    <PlatformDescription>{platformDescription}</PlatformDescription>
                </SourceDescription>
            )}
            {selectedSourceOption.secondaryDescription && (
                <>
                    <Typography.Title level={5}>Select Column</Typography.Title>
                    <SelectColumnDescription>
                        <Typography.Text type="secondary">{selectedSourceOption.secondaryDescription}</Typography.Text>
                    </SelectColumnDescription>
                </>
            )}
            {sourceType === DatasetFreshnessSourceType.FieldValue && (
                <AssertionDatasetFieldBuilder
                    name="column"
                    fields={eligibleFieldSpecs}
                    onChange={updateFieldSpec}
                    width="340px"
                    disabled={disabled}
                />
            )}
        </Form>
    );
};
