import { CaretDownFilled } from '@ant-design/icons';
import { Dropdown } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { useTranslation } from 'react-i18next';
import OptionsDropdownMenu from './OptionsDropdownMenu';
import { capitalizeFirstLetterOnly } from '../../shared/textUtil';
import { DisplayedFilterOption } from './mapFilterOption';
import { SearchFilterLabel } from './styledComponents';
import { translateDisplayNames } from '../../../utils/translation/translation';

export const IconWrapper = styled.div`
    margin-right: 8px;
    display: flex;
    svg {
        height: 14px;
        width: 14px;
    }
`;

interface Props {
    filterOptions: DisplayedFilterOption[];
    isMenuOpen: boolean;
    numActiveFilters: number;
    filterIcon: JSX.Element | null;
    displayName: string;
    searchQuery: string;
    loading: boolean;
    updateIsMenuOpen: (isOpen: boolean) => void;
    setSearchQuery: (query: string) => void;
    updateFilters: () => void;
}

export default function SearchFilterView({
    filterOptions,
    isMenuOpen,
    numActiveFilters,
    filterIcon,
    displayName,
    searchQuery,
    loading,
    updateIsMenuOpen,
    setSearchQuery,
    updateFilters,
}: Props) {
    const { t } = useTranslation();
    const translatedDisplayName = translateDisplayNames(t, displayName);

    return (
        <Dropdown
            trigger={['click']}
            menu={{ items: filterOptions }}
            open={isMenuOpen}
            onOpenChange={(open) => updateIsMenuOpen(open)}
            dropdownRender={(menu) => (
                <OptionsDropdownMenu
                    menu={menu}
                    updateFilters={updateFilters}
                    searchQuery={searchQuery}
                    updateSearchQuery={setSearchQuery}
                    isLoading={loading}
                    searchPlaceholder={translatedDisplayName}
                />
            )}
        >
            <SearchFilterLabel
                onClick={() => updateIsMenuOpen(!isMenuOpen)}
                isActive={!!numActiveFilters}
                data-testid={`filter-dropdown-${capitalizeFirstLetterOnly(translatedDisplayName)}`}
            >
                {filterIcon && <IconWrapper>{filterIcon}</IconWrapper>}
                {capitalizeFirstLetterOnly(translatedDisplayName)} {numActiveFilters ? `(${numActiveFilters}) ` : ''}
                <CaretDownFilled style={{ fontSize: '12px', height: '12px' }} />
            </SearchFilterLabel>
        </Dropdown>
    );
}
