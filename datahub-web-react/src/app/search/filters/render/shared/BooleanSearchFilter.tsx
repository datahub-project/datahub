import { CaretDownFilled } from '@ant-design/icons';
import { Dropdown } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import FilterOption from '@app/search/filters/FilterOption';
import BooleanSearchFilterMenu from '@app/search/filters/render/shared/BooleanMoreFilterMenu';
import { SearchFilterLabel } from '@app/search/filters/styledComponents';

const IconNameWrapper = styled.span`
    display: flex;
    align-items: center;
`;

const IconWrapper = styled.span`
    margin-right: 8px;
`;

interface Props {
    icon?: React.ReactNode;
    title: string;
    option: string;
    count: number;
    initialSelected: boolean;
    onUpdate: (newValue: boolean) => void;
}

export default function BooleanSearchFilter({ icon, title, option, count, initialSelected, onUpdate }: Props) {
    const [isMenuOpen, setIsMenuOpen] = useState(false);
    const [isSelected, setIsSelected] = useState<boolean>(initialSelected);

    function updateSelected() {
        onUpdate(isSelected);
        setIsMenuOpen(false);
    }

    const filterOptions = [
        {
            key: option,
            // Re-use the Normal Filter object
            label: (
                <FilterOption
                    filterOption={{ field: title, value: option, count }}
                    selectedFilterOptions={isSelected ? [{ field: title, value: option }] : []}
                    setSelectedFilterOptions={() => setIsSelected(!isSelected)}
                />
            ),
            style: { padding: 0 },
            displayName: title,
        },
    ];

    return (
        <Dropdown
            trigger={['click']}
            menu={{ items: filterOptions }}
            open={isMenuOpen}
            onOpenChange={(open) => setIsMenuOpen(open)}
            dropdownRender={(menuOption) => (
                <BooleanSearchFilterMenu menuOption={menuOption} onUpdate={updateSelected} />
            )}
        >
            <SearchFilterLabel
                onClick={() => setIsMenuOpen(!isMenuOpen)}
                isActive={isSelected}
                data-testid={`filter-dropdown-${title}`}
            >
                <IconNameWrapper>
                    {icon && <IconWrapper>{icon}</IconWrapper>}
                    {title} {isSelected ? `(1) ` : ''}
                </IconNameWrapper>
                <CaretDownFilled style={{ fontSize: '12px', height: '12px' }} />
            </SearchFilterLabel>
        </Dropdown>
    );
}
