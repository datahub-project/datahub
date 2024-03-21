/* eslint-disable import/no-cycle */
import React, { useEffect, useRef, useState } from 'react';
import { FilterField, FilterValueOption, FilterValue, FieldType } from '../types';
import TextValueMenu from './TextValueMenu';
import BooleanValueMenu from './BooleanValueMenu';
import EntityValueMenu from './EntityValueMenu';
import EntityTypeMenu from './EntityTypeMenu';
import EnumValueMenu from './EnumValueMenu';

interface Props {
    field: FilterField;
    values: FilterValue[];
    defaultOptions: FilterValueOption[];
    onChangeValues: (newValues: FilterValue[]) => void;
    type?: 'card' | 'default';
    visible: boolean;
    includeCount?: boolean;
    alignRight?: boolean;
}

export default function ValueMenu({
    field,
    values,
    defaultOptions,
    type = 'card',
    onChangeValues,
    visible,
    includeCount,
    alignRight,
}: Props) {
    const [stagedSelectedValues, setStagedSelectedValues] = useState<FilterValue[]>(values || []);
    const visibilityRef = useRef<boolean>(visible);

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

        if (!visible && previouslyVisible !== visible) {
            onChangeValues(stagedSelectedValues);
        }
    }, [visible, stagedSelectedValues, onChangeValues]);

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
                    alignRight={alignRight}
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
                    alignRight={alignRight}
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
                    alignRight={alignRight}
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
                    alignRight={alignRight}
                    onChangeValues={setStagedSelectedValues}
                    onApply={() => onChangeValues(stagedSelectedValues)}
                    includeSubTypes={false}
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
                    alignRight={alignRight}
                    onChangeValues={setStagedSelectedValues}
                    onApply={() => onChangeValues(stagedSelectedValues)}
                />
            );
        case FieldType.BROWSE_PATH:
            return <></>;
        default:
            console.error(`Unknown field type: ${field.type}`);
            return null;
    }
}
