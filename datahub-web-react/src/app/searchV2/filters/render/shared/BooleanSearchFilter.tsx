import { Dropdown, Pill } from '@components';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import FilterOption from '@app/searchV2/filters/FilterOption';
import { SearchFilterBase } from '@app/searchV2/filters/SearchFilterBase';
import BooleanSearchFilterMenu from '@app/searchV2/filters/render/shared/BooleanMoreFilterMenu';

const MenuContainer = styled.div`
    position: absolute;
    top: calc(100% + 4px);
    left: 0;
    z-index: 1050;
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

    useEffect(() => setIsSelected(initialSelected), [initialSelected]);

    function updateSelected() {
        onUpdate(isSelected);
        setIsMenuOpen(false);
    }

    return (
        <Dropdown
            open={isMenuOpen}
            onOpenChange={(isOpen) => setIsMenuOpen(isOpen)}
            dropdownRender={() => (
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
        >
            <SearchFilterBase
                onClick={() => setIsMenuOpen(!isMenuOpen)}
                isOpen={isMenuOpen}
                isActive={isSelected}
                data-testid={`filter-dropdown-${title.replace(/\s/g, '-')}`}
                onClear={() => setIsSelected(false)}
                showClear={isSelected}
            >
                {title} {isSelected ? <Pill label="1" size="sm" variant="filled" /> : ''}
            </SearchFilterBase>
        </Dropdown>
    );
}
