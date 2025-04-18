import React from 'react';

import { AssertionDatasetFieldBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/AssertionDatasetFieldBuilder';
import { getEligibleChangedRowColumns } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/field/utils';
import { AssertionMonitorBuilderState } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';
import { StopPropagation } from '@app/shared/StopPropagation';

import { useGetDatasetSchemaQuery } from '@graphql/dataset.generated';
import { DatasetFieldAssertionSourceType, SchemaField } from '@types';

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

    const selectedPath = value.parameters?.datasetFieldParameters?.changedRowsField?.path || undefined;

    return (
        <StopPropagation>
            <AssertionDatasetFieldBuilder
                selectedPath={selectedPath}
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
