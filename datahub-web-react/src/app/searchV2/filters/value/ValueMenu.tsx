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
import { useFilterMenu } from '@app/searchV2/filters/value/FilterMenuContext';
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
    // Whether the user changed the selection since it was last committed or synced. A commit can be
    // triggered twice in a row (Enter closes the menu, and closing commits), and state lags a render
    // behind, so this lives in a ref: the second call must see that there is nothing left to apply.
    const hasUncommittedChangesRef = useRef(false);
    const isDateRangeFilter = getIsDateRangeFilter(field);
    // Set when rendered in a submenu of Add filter or More, which commit staged values on close.
    const filterMenu = useFilterMenu();

    const previousValues = usePrevious(values);
    useEffect(() => {
        if (!isEqual(values, previousValues)) {
            setStagedSelectedValues(values);
            hasUncommittedChangesRef.current = false;
        }
    }, [values, previousValues]);

    const stageValues = (newValues: FilterValue[]) => {
        hasUncommittedChangesRef.current = true;
        setStagedSelectedValues(newValues);
    };

    // Commits the staged selection, then resets it to the current values. Menus that add a new
    // filter (Add filter) always receive `values={[]}`, so the reset clears them for the next use.
    const applyStagedValues = () => {
        if (!hasUncommittedChangesRef.current) return;
        hasUncommittedChangesRef.current = false;
        if (isEqual(values, stagedSelectedValues)) return;
        onChangeValues(stagedSelectedValues);
        setStagedSelectedValues(values);
    };

    // The parent menu runs the latest commit, so it sees the current staged values.
    const applyStagedValuesRef = useRef(applyStagedValues);
    applyStagedValuesRef.current = applyStagedValues;
    useEffect(() => filterMenu?.registerCommit(() => applyStagedValuesRef.current()), [filterMenu]);

    // Applies an explicit choice now: in a submenu, by closing the parent menu, which commits it.
    const applyNow = (apply: () => void) => {
        if (filterMenu) filterMenu.commitAndClose(apply);
        else apply();
    };

    const submitStagedValues = () => applyNow(applyStagedValues);

    const wrapperRef = useRef(null);
    const isRendered = useIsRendered(wrapperRef);
    const wasRenderedRef = useRef<boolean>(isRendered);

    // A popover menu is hidden, not unmounted, when it closes: apply on that transition.
    useEffect(() => {
        const wasRendered = wasRenderedRef.current;
        wasRenderedRef.current = isRendered;

        if (wasRendered && !isRendered && !isDateRangeFilter && !filterMenu) {
            applyStagedValues();
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps -- only the open-to-closed transition should apply
    }, [isRendered]);

    if (isDateRangeFilter && manuallyUpdateFilters) {
        return (
            <DateRangeMenu
                field={field}
                manuallyUpdateFilters={(newValues) => applyNow(() => manuallyUpdateFilters(newValues))}
            />
        );
    }

    const renderValueMenu = () => {
        switch (field.type) {
            case FieldType.TEXT:
                return (
                    <TextValueMenu
                        field={field}
                        values={stagedSelectedValues}
                        onChangeValues={stageValues}
                        onSubmit={submitStagedValues}
                        isRenderedInSubMenu={isRenderedInSubMenu}
                    />
                );
            case FieldType.BOOLEAN:
                return (
                    <BooleanValueMenu
                        field={field}
                        values={stagedSelectedValues}
                        className={className}
                        onChangeValues={stageValues}
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
                        onChangeValues={stageValues}
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
                        onChangeValues={stageValues}
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
                        onChangeValues={stageValues}
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
                        onChangeValues={stageValues}
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
                        onChangeValues={(newValues) => applyNow(() => onChangeValues(newValues))}
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
