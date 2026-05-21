/* eslint-disable import/no-cycle */
import { isEqual } from 'lodash';
import React, { useEffect, useRef, useState } from 'react';

import { useIsVisible } from '@components/components/Select/private/hooks/useIsVisible';

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
    includeCount?: boolean;
    className?: string;
    manuallyUpdateFilters?: (newValues: FacetFilterInput[]) => void;
    aggregationsEntityTypes?: Array<EntityType>;
    isRenderedInSubMenu?: boolean;
}

export default function ValueMenu({
    field,
    values,
    defaultOptions,
    onChangeValues,
    includeCount,
    className,
    manuallyUpdateFilters,
    aggregationsEntityTypes,
    isRenderedInSubMenu = false,
}: Props) {
    const [stagedSelectedValues, setStagedSelectedValues] = useState<FilterValue[]>(values || []);
    const isDateRangeFilter = getIsDateRangeFilter(field);

    const previousValues = usePrevious(values);
    useEffect(() => {
        if (!isEqual(values, previousValues)) {
            setStagedSelectedValues(values);
        }
    }, [values, previousValues]);

    const wrapperRef = useRef(null);
    const isVisible = useIsVisible(wrapperRef);
    const isVisibleRef = useRef<boolean>(isVisible);

    useEffect(() => {
        const previouslyVisible = isVisibleRef.current;
        isVisibleRef.current = isVisible;

        if (
            !isVisible &&
            previouslyVisible !== isVisible &&
            !isDateRangeFilter &&
            !isEqual(values, stagedSelectedValues)
        ) {
            onChangeValues(stagedSelectedValues);
        }
    }, [isVisible, stagedSelectedValues, onChangeValues, isDateRangeFilter, values]);

    if (isDateRangeFilter && manuallyUpdateFilters) {
        return <DateRangeMenu field={field} manuallyUpdateFilters={manuallyUpdateFilters} />;
    }

    const renderValueMenu = () => {
        switch (field.type) {
            case FieldType.TEXT:
                return (
                    <TextValueMenu
                        field={field}
                        values={stagedSelectedValues}
                        onChangeValues={setStagedSelectedValues}
                        isRenderedInSubMenu={isRenderedInSubMenu}
                    />
                );
            case FieldType.BOOLEAN:
                return (
                    <BooleanValueMenu
                        field={field}
                        values={stagedSelectedValues}
                        className={className}
                        onChangeValues={setStagedSelectedValues}
                        isRenderedInSubMenu={isRenderedInSubMenu}
                    />
                );
            case FieldType.ENTITY:
                return (
                    <EntityValueMenu
                        field={field}
                        includeCount={includeCount}
                        values={stagedSelectedValues}
                        defaultOptions={defaultOptions}
                        className={className}
                        onChangeValues={setStagedSelectedValues}
                        isRenderedInSubMenu={isRenderedInSubMenu}
                    />
                );
            case FieldType.NESTED_ENTITY_TYPE:
                return (
                    <EntityTypeMenu
                        field={field}
                        includeCount={includeCount}
                        values={stagedSelectedValues}
                        defaultOptions={defaultOptions}
                        className={className}
                        onChangeValues={setStagedSelectedValues}
                        isRenderedInSubMenu={isRenderedInSubMenu}
                    />
                );
            case FieldType.ENTITY_TYPE:
                return (
                    <EntityTypeMenu
                        field={field}
                        includeCount={includeCount}
                        values={stagedSelectedValues}
                        defaultOptions={defaultOptions}
                        className={className}
                        onChangeValues={setStagedSelectedValues}
                        aggregationsEntityTypes={aggregationsEntityTypes}
                        isRenderedInSubMenu={isRenderedInSubMenu}
                    />
                );
            case FieldType.ENUM:
                return (
                    <EnumValueMenu
                        field={field}
                        includeCount={includeCount}
                        values={stagedSelectedValues}
                        defaultOptions={defaultOptions}
                        className={className}
                        onChangeValues={setStagedSelectedValues}
                        aggregationsEntityTypes={aggregationsEntityTypes}
                        isRenderedInSubMenu={isRenderedInSubMenu}
                    />
                );
            case FieldType.BUCKETED_TIMESTAMP:
                return (
                    <TimeBucketMenu
                        field={field}
                        values={stagedSelectedValues}
                        className={className}
                        onChangeValues={onChangeValues}
                        isRenderedInSubMenu={isRenderedInSubMenu}
                    />
                );
            case FieldType.BROWSE_PATH:
                return <></>;
            default:
                console.error(`Unknown field type: ${field}`);
                return null;
        }
    };

    return <div ref={wrapperRef}>{renderValueMenu()}</div>;
}
