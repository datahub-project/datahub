/* eslint-disable import/no-cycle */
import { isEqual } from 'lodash';
import React, { useEffect, useRef, useState } from 'react';

import { FieldType, FilterField, FilterValue, FilterValueOption } from '@app/searchV2/filters/types';
import { getIsDateRangeFilter } from '@app/searchV2/filters/utils';
import BooleanValueMenu from '@app/searchV2/filters/value/BooleanValueMenu';
import DateRangeMenu from '@app/searchV2/filters/value/DateRangeMenu';
import EntityTypeMenu from '@app/searchV2/filters/value/EntityTypeMenu';
import EntityValueMenu from '@app/searchV2/filters/value/EntityValueMenu';
import EnumValueMenu from '@app/searchV2/filters/value/EnumValueMenu';
import TextValueMenu from '@app/searchV2/filters/value/TextValueMenu';
import TimeBucketMenu from '@app/searchV2/filters/value/TimeBucketMenu';
import usePrevious from '@app/shared/usePrevious';
import { EntityType, FacetFilterInput } from '@src/types.generated';

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
    aggregationsEntityTypes?: Array<EntityType>;
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
    aggregationsEntityTypes,
}: Props) {
    const [stagedSelectedValues, setStagedSelectedValues] = useState<FilterValue[]>(values || []);
    const visibilityRef = useRef<boolean>(visible);
    const isDateRangeFilter = getIsDateRangeFilter(field);

    /**
     * Synchronize stagedSelectedValues with the values prop
     * NOTE: Callback with useState not feasible due to its initialization behavior.
     */
    const previousValues = usePrevious(values);
    useEffect(() => {
        if (!isEqual(values, previousValues)) {
            setStagedSelectedValues(values);
        }
    }, [values, previousValues]);

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
                    aggregationsEntityTypes={aggregationsEntityTypes}
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
                    aggregationsEntityTypes={aggregationsEntityTypes}
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
