import React, { useEffect } from 'react';

import { EvaluationScheduleBuilder } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/common/EvaluationScheduleBuilder';
import { convertSchemaMetadataToAssertionFields } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/field/utils';
import { CompatibilityBuilder } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/schema/CompatibilityBuilder';
import { SchemaBuilder } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/steps/schema/SchemaBuilder';
import { AssertionMonitorBuilderState } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/types';

import { useGetDatasetSchemaQuery } from '@graphql/dataset.generated';
import {
    AssertionType,
    CronSchedule,
    SchemaAssertionCompatibility,
    SchemaAssertionField,
    SchemaMetadata,
} from '@types';

type Props = {
    state: AssertionMonitorBuilderState;
    updateState: (state: AssertionMonitorBuilderState) => void;
    disabled?: boolean;
};

/**
 * Build a new schema assertion
 */
export const SchemaAssertionBuilder = ({ state, updateState, disabled }: Props) => {
    // 2 things:
    // 1. Compatibility Select
    // 2. Columns selection.

    const { data } = useGetDatasetSchemaQuery({
        variables: {
            urn: state.entityUrn as string,
        },
        fetchPolicy: 'cache-first',
    });
    const schemaMetadata = data?.dataset?.schemaMetadata;
    const schedule: CronSchedule | undefined | null = state?.schedule;

    useEffect(() => {
        const schemaFields =
            (schemaMetadata && convertSchemaMetadataToAssertionFields(schemaMetadata as SchemaMetadata)) || [];
        // Set the original fields to the actual schema fields.
        if (schemaFields.length && !state?.assertion?.schemaAssertion?.fields?.length) {
            updateState({
                ...state,
                assertion: {
                    ...state.assertion,
                    schemaAssertion: {
                        ...state.assertion?.schemaAssertion,
                        fields: schemaFields,
                    },
                },
            });
        }
    }, [schemaMetadata, state, updateState]);

    const onChangeCompatibility = (newCompatibility: SchemaAssertionCompatibility) => {
        updateState({
            ...state,
            assertion: {
                ...state.assertion,
                schemaAssertion: {
                    ...state.assertion?.schemaAssertion,
                    compatibility: newCompatibility,
                },
            },
        });
    };

    const onChangeFields = (newFields: Partial<SchemaAssertionField>[]) => {
        updateState({
            ...state,
            assertion: {
                ...state.assertion,
                schemaAssertion: {
                    ...state.assertion?.schemaAssertion,
                    fields: newFields,
                },
            },
        });
    };

    const updateAssertionSchedule = (newSchedule: CronSchedule) => {
        updateState({
            ...state,
            schedule: newSchedule,
        });
    };

    const existingSchemaFields =
        (schemaMetadata && convertSchemaMetadataToAssertionFields(schemaMetadata as SchemaMetadata)) || [];
    const compatibility = state?.assertion?.schemaAssertion?.compatibility;
    const fields = state?.assertion?.schemaAssertion?.fields;

    return (
        <>
            <CompatibilityBuilder selected={compatibility} onChange={onChangeCompatibility} disabled={disabled} />
            <SchemaBuilder
                selected={fields || []}
                onChange={onChangeFields}
                disabled={disabled}
                options={existingSchemaFields}
            />
            <EvaluationScheduleBuilder
                value={schedule}
                assertionType={AssertionType.DataSchema}
                onChange={updateAssertionSchedule}
                disabled={disabled}
                showAdvanced={false}
            />
        </>
    );
};
