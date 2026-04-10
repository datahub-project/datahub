/* eslint-disable import/no-cycle */
import { Dropdown } from '@components';
import React, { CSSProperties, useCallback, useEffect, useRef, useState } from 'react';
import styled from 'styled-components';

import { FilterField, FilterValue, FilterValueOption } from '@app/searchV2/filters/types';
import ValueMenu from '@app/searchV2/filters/value/ValueMenu';
import { EntityType, FacetFilterInput } from '@src/types.generated';

const MenuContainer = styled.div`
    position: absolute;
    top: calc(100% + 4px);
    left: 0;
    z-index: 1050;
`;

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

    const wrapperRef = useRef<HTMLDivElement>(null);

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
        <Dropdown
            open={isMenuOpen}
            onOpenChange={(newOpen) => updateIsOpen(newOpen)}
            dropdownRender={() => {
                return (
                    <MenuContainer style={menuStyle} ref={wrapperRef}>
                        <ValueMenu
                            field={field}
                            values={values}
                            defaultOptions={defaultOptions}
                            onChangeValues={onUpdateValues}
                            includeCount
                            manuallyUpdateFilters={onManuallyUpdateFilters}
                            aggregationsEntityTypes={aggregationsEntityTypes}
                        />
                    </MenuContainer>
                );
            }}
        >
            {children}
        </Dropdown>
    );
}
