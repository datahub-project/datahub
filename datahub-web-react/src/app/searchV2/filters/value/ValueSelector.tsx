/* eslint-disable import/no-cycle */
import React, { CSSProperties, useCallback, useMemo, useRef, useState } from 'react';
import styled from 'styled-components';

import useClickOutside from '@components/components/Utils/ClickOutside/useClickOutside';

import { FilterField, FilterValue, FilterValueOption } from '@app/searchV2/filters/types';
import ValueMenu from '@app/searchV2/filters/value/ValueMenu';
import { EntityType, FacetFilterInput } from '@src/types.generated';

const Wrapper = styled.div`
    position: relative;
`;

const MenuContainer = styled.div`
    position: absolute;
    top: calc(100% + 4px);
    left: 0;
    z-index: 1050;
`;

interface Props {
    field: FilterField;
    values: FilterValue[];
    defaultOptions: FilterValueOption[];
    onChangeValues: (newValues: FilterValue[]) => void;
    children?: any;
    className?: string;
    menuStyle?: CSSProperties;
    manuallyUpdateFilters?: (newValues: FacetFilterInput[]) => void;
    aggregationsEntityTypes?: Array<EntityType>;
}

export default function ValueSelector({
    field,
    values,
    defaultOptions,
    onChangeValues,
    children,
    className,
    menuStyle,
    manuallyUpdateFilters,
    aggregationsEntityTypes,
}: Props) {
    const [isMenuOpen, setIsMenuOpen] = useState(false);
    const wrapperRef = useRef<HTMLDivElement>(null);

    const onUpdateValues = (newValues: FilterValue[]) => {
        setIsMenuOpen(false);
        onChangeValues(newValues);
    };

    const onManuallyUpdateFilters = (newValues: FacetFilterInput[]) => {
        setIsMenuOpen(false);
        manuallyUpdateFilters?.(newValues);
    };

    const handleClickOutside = useCallback(() => setIsMenuOpen(false), []);
    const clickOutsideOptions = useMemo(() => ({ wrappers: [wrapperRef] }), []);
    useClickOutside(handleClickOutside, clickOutsideOptions);

    return (
        <Wrapper ref={wrapperRef} className={className}>
            <div
                role="button"
                tabIndex={0}
                onClick={() => setIsMenuOpen((prev) => !prev)}
                onKeyDown={(e) => {
                    if (e.key === 'Enter' || e.key === ' ') setIsMenuOpen((prev) => !prev);
                }}
            >
                {children}
            </div>
            {isMenuOpen && (
                <MenuContainer style={menuStyle}>
                    <ValueMenu
                        field={field}
                        values={values}
                        defaultOptions={defaultOptions}
                        onChangeValues={onUpdateValues}
                        visible={isMenuOpen}
                        includeCount
                        manuallyUpdateFilters={onManuallyUpdateFilters}
                        aggregationsEntityTypes={aggregationsEntityTypes}
                    />
                </MenuContainer>
            )}
        </Wrapper>
    );
}
