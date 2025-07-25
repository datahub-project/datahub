import { Button, Tooltip } from '@components';
import { Form } from 'antd';
import Typography from 'antd/lib/typography';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { useConnectionForEntityExists } from '@app/entity/shared/tabs/Dataset/Validations/acrylUtils';
import { AssertionDatasetFieldBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/AssertionDatasetFieldBuilder';
import {
    getEligibleFieldColumns,
    getFieldAssertionTypeKey,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/field/utils';
import { AssertionMonitorBuilderState } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';
import { CreateBulkFieldSmartAssertionsModal } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/bulk_builder/bulk_fields/CreateBulkFieldSmartAssertionsModal';
import { useAppConfig } from '@app/useAppConfig';

import { useGetDatasetSchemaQuery } from '@graphql/dataset.generated';
import { FieldAssertionType, SchemaField } from '@types';

const MultiCreateCTAWrapper = styled.div`
    display: flex;
    align-items: center;
    margin-top: 4px;
`;

const Section = styled.div`
    margin: 16px 0;
`;

type Props = {
    value: AssertionMonitorBuilderState;
    onChange: (newState: AssertionMonitorBuilderState) => void;
    disabled?: boolean;
    isEditMode?: boolean;
    onCloseDrawer?: (skipConfirmation?: boolean) => void;
};

export const FieldColumnBuilder = ({ value, onChange, disabled, isEditMode, onCloseDrawer }: Props) => {
    const isOnlineSmartAssertionsEnabled = useAppConfig().config.featureFlags.onlineSmartAssertionsEnabled;
    const connectionForEntityExists = useConnectionForEntityExists(value.entityUrn || '');
    const [isBulkSmartAssertionsModalOpen, setIsBulkSmartAssertionsModalOpen] = useState(false);

    const form = Form.useFormInstance();
    const fieldAssertionType = value.assertion?.fieldAssertion?.type;
    const fieldAssertionTypeKey = getFieldAssertionTypeKey(fieldAssertionType);
    const fieldColumn = value.assertion?.fieldAssertion?.[fieldAssertionTypeKey]?.field?.path;
    const { data } = useGetDatasetSchemaQuery({
        variables: {
            urn: value.entityUrn as string,
        },
        fetchPolicy: 'cache-first',
    });

    const columnOptions = data?.dataset?.schemaMetadata?.fields
        ? getEligibleFieldColumns(data.dataset.schemaMetadata.fields as SchemaField[])
        : [];

    const updateColumnSpec = (newFieldPath: string) => {
        const fieldSpec = columnOptions.find((field) => field.path === newFieldPath);

        onChange({
            ...value,
            assertion: {
                ...value.assertion,
                fieldAssertion: {
                    ...value.assertion?.fieldAssertion,
                    [fieldAssertionTypeKey]: {
                        ...value.assertion?.fieldAssertion?.[fieldAssertionTypeKey],
                        field: fieldSpec,
                        operator: undefined,
                        metric: undefined,
                    },
                },
            },
        });
    };

    useEffect(() => {
        form.setFieldValue('fieldColumn', fieldColumn);
    }, [form, fieldColumn]);

    const canMultiCreateSmartAssertion =
        !isEditMode && fieldAssertionType === FieldAssertionType.FieldMetric && isOnlineSmartAssertionsEnabled;

    return (
        <Section>
            <Typography.Title level={5}>Column</Typography.Title>
            <Typography.Paragraph type="secondary">Select a column to check</Typography.Paragraph>
            <AssertionDatasetFieldBuilder
                selectedPath={fieldColumn || undefined}
                name="fieldColumn"
                width="240px"
                fields={columnOptions}
                onChange={updateColumnSpec}
                disabled={disabled}
                showSearch
            />

            {canMultiCreateSmartAssertion && (
                <MultiCreateCTAWrapper>
                    <Typography.Paragraph type="secondary" style={{ margin: 0 }}>
                        Or
                    </Typography.Paragraph>
                    <Tooltip
                        title={
                            !connectionForEntityExists
                                ? 'Could not find a valid connection to this platform.'
                                : undefined
                        }
                    >
                        <div>
                            <Button
                                type="button"
                                variant="text"
                                onClick={() => {
                                    setIsBulkSmartAssertionsModalOpen(true);
                                    analytics.event({
                                        type: EventType.ClickBulkCreateAssertion,
                                        surface: 'field-metric-assertion-builder',
                                        entityUrn: value.entityUrn,
                                    });
                                }}
                                style={{ paddingLeft: 2, paddingRight: 2, marginLeft: 4 }}
                                disabled={connectionForEntityExists}
                            >
                                Bulk-Create Smart Assertions
                            </Button>
                        </div>
                    </Tooltip>
                </MultiCreateCTAWrapper>
            )}
            <CreateBulkFieldSmartAssertionsModal
                entityUrn={value.entityUrn || ''}
                open={isBulkSmartAssertionsModalOpen}
                onClose={() => setIsBulkSmartAssertionsModalOpen(false)}
                columns={columnOptions}
                onSuccess={() => {
                    setIsBulkSmartAssertionsModalOpen(false);
                    onCloseDrawer?.(true);
                }}
            />
        </Section>
    );
};
