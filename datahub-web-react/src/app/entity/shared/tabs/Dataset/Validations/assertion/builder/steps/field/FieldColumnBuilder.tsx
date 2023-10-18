import React from 'react';
import Typography from 'antd/lib/typography';
import styled from 'styled-components';
import { AssertionMonitorBuilderState } from '../../types';
import { useGetDatasetSchemaQuery } from '../../../../../../../../../../graphql/dataset.generated';
import { SchemaField } from '../../../../../../../../../../types.generated';
import { getEligibleFieldColumns } from './utils';
import { AssertionDatasetFieldBuilder } from '../AssertionDatasetFieldBuilder';

const Section = styled.div`
    margin: 16px 0;
`;

type Props = {
    value: AssertionMonitorBuilderState;
    onChange: (newState: AssertionMonitorBuilderState) => void;
};

export const FieldColumnBuilder = ({ value, onChange }: Props) => {
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
                    fieldValuesAssertion: {
                        ...value.assertion?.fieldAssertion?.fieldValuesAssertion,
                        field: fieldSpec,
                        operator: undefined,
                    },
                },
            },
        });
    };

    return (
        <Section>
            <Typography.Title level={5}>Column</Typography.Title>
            <Typography.Paragraph type="secondary">Select a column to check</Typography.Paragraph>
            <AssertionDatasetFieldBuilder
                name="fieldColumn"
                width="240px"
                fields={columnOptions}
                onChange={updateColumnSpec}
            />
        </Section>
    );
};
