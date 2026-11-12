/* eslint-disable import/no-cycle */
import isEqual from 'lodash/isEqual';
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
import { useIsRendered } from '@app/searchV2/filters/value/useIsRendered';
import usePrevious from '@app/sharedV2/usePrevious';
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
    showDefaultOptions?: boolean;
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
    showDefaultOptions,
}: Props) {
    const [stagedSelectedValues, setStagedSelectedValues] = useState<FilterValue[]>(values || []);
    const isDateRangeFilter = getIsDateRangeFilter(field);

    const previousValues = usePrevious(values);
    useEffect(() => {
        if (!isEqual(values, previousValues)) {
            setStagedSelectedValues(values);
        }
    }, [values, previousValues]);

    // Commits the staged selection, then resets it to the current values. Menus that add a new
    // filter (Add filter) always receive `values={[]}`, so without the reset the same selection
    // would be committed again the next time the menu closes.
    const applyStagedValues = () => {
        if (isEqual(values, stagedSelectedValues)) return;
        onChangeValues(stagedSelectedValues);
        setStagedSelectedValues(values);
    };

    const wrapperRef = useRef(null);
    const isRendered = useIsRendered(wrapperRef);
    const wasRenderedRef = useRef<boolean>(isRendered);

    // The menu is hidden, not unmounted, when its popover or submenu closes: apply on that transition.
    useEffect(() => {
        const wasRendered = wasRenderedRef.current;
        wasRenderedRef.current = isRendered;

        if (wasRendered && !isRendered && !isDateRangeFilter) {
            applyStagedValues();
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps -- only the open-to-closed transition should apply
    }, [isRendered]);

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
                        onSubmit={applyStagedValues}
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
                        showDefaultOptions={showDefaultOptions}
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
                        showDefaultOptions={showDefaultOptions}
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
                        showDefaultOptions={showDefaultOptions}
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
