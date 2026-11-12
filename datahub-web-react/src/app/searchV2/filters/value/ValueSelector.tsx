/* eslint-disable import/no-cycle */
import { Popover } from '@components';
import React, { CSSProperties, useCallback, useEffect, useState } from 'react';

import { FilterField, FilterValue, FilterValueOption } from '@app/searchV2/filters/types';
import ValueMenu from '@app/searchV2/filters/value/ValueMenu';
import { EntityType, FacetFilterInput } from '@src/types.generated';

// The value menus draw their own card, so the popover's default chrome is removed.
const OVERLAY_INNER_STYLE: CSSProperties = {
    padding: 0,
    background: 'transparent',
    boxShadow: 'none',
    borderRadius: 0,
    overflow: 'visible',
};

interface Props {
    isOpen?: boolean;
    setIsOpen?: React.Dispatch<React.SetStateAction<boolean>>;
    field: FilterField;
    values: FilterValue[];
    defaultOptions: FilterValueOption[];
    onChangeValues: (newValues: FilterValue[]) => void;
    children?: any;
    menuStyle?: CSSProperties;
    manuallyUpdateFilters?: (newValues: FacetFilterInput[]) => void;
    aggregationsEntityTypes?: Array<EntityType>;
    isRenderedInSubMenu?: boolean;
    showDefaultOptions?: boolean;
}

export default function ValueSelector({
    isOpen,
    setIsOpen,
    field,
    values,
    defaultOptions,
    onChangeValues,
    children,
    menuStyle,
    manuallyUpdateFilters,
    aggregationsEntityTypes,
    isRenderedInSubMenu,
    showDefaultOptions,
}: Props) {
    const [isMenuOpen, setIsMenuOpen] = useState<boolean>(isOpen ?? false);

    useEffect(() => {
        if (isOpen !== undefined && isOpen !== isMenuOpen) setIsMenuOpen(isOpen);
    }, [isOpen, isMenuOpen]);

    const updateIsOpen = useCallback(
        (newIsOpen: boolean) => {
            setIsMenuOpen(newIsOpen);
            setIsOpen?.(newIsOpen);
        },
        [setIsOpen],
    );

    const onUpdateValues = (newValues: FilterValue[]) => {
        updateIsOpen(false);
        onChangeValues(newValues);
    };

    const onManuallyUpdateFilters = (newValues: FacetFilterInput[]) => {
        updateIsOpen(false);
        manuallyUpdateFilters?.(newValues);
    };

    if (isRenderedInSubMenu) {
        return (
            <ValueMenu
                field={field}
                values={values}
                defaultOptions={defaultOptions}
                onChangeValues={onUpdateValues}
                includeCount
                manuallyUpdateFilters={onManuallyUpdateFilters}
                aggregationsEntityTypes={aggregationsEntityTypes}
                isRenderedInSubMenu
            />
        );
    }

    return (
        <Popover
            open={isMenuOpen}
            onOpenChange={updateIsOpen}
            trigger="click"
            placement="bottomLeft"
            overlayStyle={menuStyle}
            overlayInnerStyle={OVERLAY_INNER_STYLE}
            // ValueMenu applies its staged selection when it becomes hidden, so it must stay mounted.
            destroyTooltipOnHide={false}
            content={
                <ValueMenu
                    field={field}
                    values={values}
                    defaultOptions={defaultOptions}
                    onChangeValues={onUpdateValues}
                    includeCount
                    manuallyUpdateFilters={onManuallyUpdateFilters}
                    aggregationsEntityTypes={aggregationsEntityTypes}
                    showDefaultOptions={showDefaultOptions}
                />
            }
        >
            {children}
        </Popover>
    );
}
