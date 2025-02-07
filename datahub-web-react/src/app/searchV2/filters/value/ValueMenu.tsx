/* eslint-disable import/no-cycle */
import TimeBucketMenu from '@app/searchV2/filters/value/TimeBucketMenu';
import { FacetFilterInput } from '@src/types.generated';
import React, { useEffect, useRef, useState } from 'react';
import { FilterField, FilterValueOption, FilterValue, FieldType } from '../types';
import TextValueMenu from './TextValueMenu';
import BooleanValueMenu from './BooleanValueMenu';
import EntityValueMenu from './EntityValueMenu';
import EntityTypeMenu from './EntityTypeMenu';
import EnumValueMenu from './EnumValueMenu';
import DateRangeMenu from './DateRangeMenu';
import { getIsDateRangeFilter } from '../utils';

interface Props {
    field: FilterField;
    values: FilterValue[];
    defaultOptions: FilterValueOption[];
    onChangeValues: (newValues: FilterValue[]) => void;
    type?: 'card' | 'default';
    visible: boolean;
    includeCount?: boolean;
    className?: string;
    manuallyUpdateFilters?: (newValues: FacetFilterInput[]) => void;
}

export default function ValueMenu({
    field,
    values,
    defaultOptions,
    type = 'card',
    onChangeValues,
    visible,
    includeCount,
    className,
    manuallyUpdateFilters,
}: Props) {
    const [stagedSelectedValues, setStagedSelectedValues] = useState<FilterValue[]>(values || []);
    const visibilityRef = useRef<boolean>(visible);
    const isDateRangeFilter = getIsDateRangeFilter(field);

    /**
     * Synchronize stagedSelectedValues with the values prop
     * NOTE: Callback with useState not feasible due to its initialization behavior.
     */
    useEffect(() => {
        setStagedSelectedValues(values);
    }, [values]);

    /**
     * If the visibility of the menu changes in the parent component, we can dump off the staged values before closing
     * to make the UI feel more responsive.
     */
    useEffect(() => {
        const previouslyVisible = visibilityRef.current;
        visibilityRef.current = visible;

        if (!visible && previouslyVisible !== visible && !isDateRangeFilter) {
            onChangeValues(stagedSelectedValues);
        }
    }, [visible, stagedSelectedValues, onChangeValues, isDateRangeFilter]);

    if (isDateRangeFilter && manuallyUpdateFilters) {
        return <DateRangeMenu field={field} manuallyUpdateFilters={manuallyUpdateFilters} />;
    }

    switch (field.type) {
        case FieldType.TEXT:
            return (
                <TextValueMenu
                    field={field}
                    values={stagedSelectedValues}
                    onChangeValues={setStagedSelectedValues}
                    onApply={() => onChangeValues(stagedSelectedValues)}
                />
            );
        case FieldType.BOOLEAN:
            return (
                <BooleanValueMenu
                    field={field}
                    values={stagedSelectedValues}
                    type={type}
                    className={className}
                    onChangeValues={setStagedSelectedValues}
                    onApply={() => onChangeValues(stagedSelectedValues)}
                />
            );
        case FieldType.ENTITY:
            return (
                <EntityValueMenu
                    field={field}
                    includeCount={includeCount}
                    values={stagedSelectedValues}
                    defaultOptions={defaultOptions}
                    type={type}
                    className={className}
                    onChangeValues={setStagedSelectedValues}
                    onApply={() => onChangeValues(stagedSelectedValues)}
                />
            );
        case FieldType.NESTED_ENTITY_TYPE:
            return (
                <EntityTypeMenu
                    field={field}
                    includeCount={includeCount}
                    values={stagedSelectedValues}
                    defaultOptions={defaultOptions}
                    type={type}
                    className={className}
                    onChangeValues={setStagedSelectedValues}
                    onApply={() => onChangeValues(stagedSelectedValues)}
                />
            );
        case FieldType.ENTITY_TYPE:
            return (
                <EntityTypeMenu
                    field={field}
                    includeCount={includeCount}
                    values={stagedSelectedValues}
                    defaultOptions={defaultOptions}
                    type={type}
                    className={className}
                    onChangeValues={setStagedSelectedValues}
                    onApply={() => onChangeValues(stagedSelectedValues)}
                />
            );
        case FieldType.ENUM:
            return (
                <EnumValueMenu
                    field={field}
                    includeCount={includeCount}
                    values={stagedSelectedValues}
                    defaultOptions={defaultOptions}
                    type={type}
                    className={className}
                    onChangeValues={setStagedSelectedValues}
                    onApply={() => onChangeValues(stagedSelectedValues)}
                />
            );
        case FieldType.BUCKETED_TIMESTAMP:
            return (
                <TimeBucketMenu
                    field={field}
                    values={stagedSelectedValues}
                    type={type}
                    className={className}
                    onChangeValues={setStagedSelectedValues}
                    onApply={() => onChangeValues(stagedSelectedValues)}
                />
            );
        case FieldType.BROWSE_PATH:
            return <></>;
        default:
            console.error(`Unknown field type: ${field}`);
            return null;
    }
}
