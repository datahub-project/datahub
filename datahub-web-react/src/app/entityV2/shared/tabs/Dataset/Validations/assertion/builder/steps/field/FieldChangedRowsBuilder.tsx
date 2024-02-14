import React from 'react';
import { AssertionMonitorBuilderState } from '../../types';
import { DatasetFieldAssertionSourceType, SchemaField } from '../../../../../../../../../../types.generated';
import { useGetDatasetSchemaQuery } from '../../../../../../../../../../graphql/dataset.generated';
import { getEligibleChangedRowColumns } from './utils';
import { StopPropagation } from '../../../../../../../../../shared/StopPropagation';
import { AssertionDatasetFieldBuilder } from '../AssertionDatasetFieldBuilder';

type Props = {
    value: AssertionMonitorBuilderState;
    onChange: (newState: AssertionMonitorBuilderState) => void;
    disabled?: boolean;
};

export const FieldChangedRowsBuilder = ({ value, onChange, disabled }: Props) => {
    const sourceType = value.parameters?.datasetFieldParameters?.sourceType;
    const { data } = useGetDatasetSchemaQuery({
        variables: {
            urn: value.entityUrn as string,
        },
        fetchPolicy: 'cache-first',
    });

    const columnOptions = data?.dataset?.schemaMetadata?.fields
        ? getEligibleChangedRowColumns(data.dataset.schemaMetadata.fields as SchemaField[])
        : [];

    const updateChangedRows = (newPath: string) => {
        const newSpec = columnOptions.filter((field) => field.path === newPath)[0];
        onChange({
            ...value,
            parameters: {
                ...value.parameters,
                datasetFieldParameters: {
                    ...value.parameters?.datasetFieldParameters,
                    changedRowsField: newSpec,
                },
            },
        });
    };

    return (
        <StopPropagation>
            <AssertionDatasetFieldBuilder
                name="changedRowsColumn"
                fields={columnOptions}
                onChange={updateChangedRows}
                width="240px"
                required={sourceType === DatasetFieldAssertionSourceType.ChangedRowsQuery}
                disabled={disabled || sourceType !== DatasetFieldAssertionSourceType.ChangedRowsQuery}
                placeholder="Select a High Watermark column"
            />
        </StopPropagation>
    );
};
