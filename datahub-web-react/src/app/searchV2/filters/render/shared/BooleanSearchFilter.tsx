import { CaretDown } from '@phosphor-icons/react';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import styled from 'styled-components';

import useClickOutside from '@components/components/Utils/ClickOutside/useClickOutside';

import FilterOption from '@app/searchV2/filters/FilterOption';
import BooleanSearchFilterMenu from '@app/searchV2/filters/render/shared/BooleanMoreFilterMenu';
import { SearchFilterLabel } from '@app/searchV2/filters/styledComponents';

const DropdownWrapper = styled.div`
    position: relative;
`;

const MenuContainer = styled.div`
    position: absolute;
    top: calc(100% + 4px);
    left: 0;
    z-index: 1050;
`;

const CaretIcon = styled(CaretDown)<{ $isOpen?: boolean }>`
    transition: transform 0.2s ease;
    ${(props) => props.$isOpen && 'transform: rotate(180deg);'}
`;

interface Props {
    title: string;
    option: string;
    count: number;
    initialSelected: boolean;
    onUpdate: (newValue: boolean) => void;
}

export default function BooleanSearchFilter({ title, option, count, initialSelected, onUpdate }: Props) {
    const [isMenuOpen, setIsMenuOpen] = useState(false);
    const [isSelected, setIsSelected] = useState<boolean>(initialSelected);
    const wrapperRef = useRef<HTMLDivElement>(null);

    useEffect(() => setIsSelected(initialSelected), [initialSelected]);

    function updateSelected() {
        onUpdate(isSelected);
        setIsMenuOpen(false);
    }

    const handleClickOutside = useCallback(() => setIsMenuOpen(false), []);
    const clickOutsideOptions = useMemo(() => ({ wrappers: [wrapperRef] }), []);
    useClickOutside(handleClickOutside, clickOutsideOptions);

    return (
        <DropdownWrapper ref={wrapperRef}>
            <SearchFilterLabel
                onClick={() => setIsMenuOpen(!isMenuOpen)}
                $isActive={isSelected}
                data-testid={`filter-dropdown-${title.replace(/\s/g, '-')}`}
            >
                {title} {isSelected ? `(1) ` : ''}
                <CaretIcon size={12} $isOpen={isMenuOpen} />
            </SearchFilterLabel>
            {isMenuOpen && (
                <MenuContainer>
                    <BooleanSearchFilterMenu
                        filterOption={
                            <FilterOption
                                filterOption={{ field: title, value: option, count }}
                                selectedFilterOptions={isSelected ? [{ field: title, value: option }] : []}
                                setSelectedFilterOptions={() => setIsSelected(!isSelected)}
                            />
                        }
                        onUpdate={updateSelected}
                    />
                </MenuContainer>
            )}
        </DropdownWrapper>
    );
}
