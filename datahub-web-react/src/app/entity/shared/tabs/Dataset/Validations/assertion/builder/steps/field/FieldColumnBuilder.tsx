import { Form } from 'antd';
import Typography from 'antd/lib/typography';
import React, { useEffect } from 'react';
import styled from 'styled-components';

import { AssertionDatasetFieldBuilder } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/AssertionDatasetFieldBuilder';
import {
    getEligibleFieldColumns,
    getFieldAssertionTypeKey,
} from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/field/utils';
import { AssertionMonitorBuilderState } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/types';

import { useGetDatasetSchemaQuery } from '@graphql/dataset.generated';
import { SchemaField } from '@types';

const Section = styled.div`
    margin: 16px 0;
`;

type Props = {
    value: AssertionMonitorBuilderState;
    onChange: (newState: AssertionMonitorBuilderState) => void;
    disabled?: boolean;
};

export const FieldColumnBuilder = ({ value, onChange, disabled }: Props) => {
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
            />
        </Section>
    );
};
