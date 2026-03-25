import { LoadingOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components/macro';

import DropdownSearchBar from '@components/components/Select/private/DropdownSearchBar';

const DropdownCard = styled.div<{ $shouldAddWrapperStyles: boolean }>`
    ${(props) =>
        props.$shouldAddWrapperStyles &&
        `
        background-color: ${props.theme.colors.bg};
        border-radius: 12px;
        box-shadow: ${props.theme.colors.shadowMd};
    `}
    overflow: hidden;
    min-width: 200px;
`;

const ScrollableContent = styled.div<{ $shouldAddWrapperStyles: boolean }>`
    max-height: 360px;
    overflow: auto;
    min-width: 260px;
    display: flex;
    flex-direction: column;
    gap: 4px;
    ${(props) => props.$shouldAddWrapperStyles && `padding: 4px;`}
`;

const SearchBarWrapper = styled.div<{ $shouldAddWrapperStyles: boolean }>`
    padding: ${(props) => (props.$shouldAddWrapperStyles ? '8px 8px 4px 8px' : '4px')};
`;

const LoadingWrapper = styled.div`
    display: flex;
    justify-content: center;
    padding: 9px;
    color: ${(props) => props.theme.colors.textSecondary};

    svg {
        height: 16px;
        width: 16px;
    }
`;

interface Props {
    menu: React.ReactNode;
    isLoading?: boolean;
    searchQuery?: string;
    updateSearchQuery?: (query: string) => void;
    searchPlaceholder?: string;
    showSearchBar?: boolean;
    className?: string;
    isRenderedInSubMenu?: boolean;
}

export default function OptionsDropdownMenu({
    menu,
    isLoading,
    searchQuery,
    updateSearchQuery,
    searchPlaceholder,
    showSearchBar = true,
    className,
    isRenderedInSubMenu,
}: Props) {
    return (
        <DropdownCard
            data-testid="filter-dropdown"
            className={className}
            $shouldAddWrapperStyles={!isRenderedInSubMenu}
        >
            {showSearchBar && (
                <SearchBarWrapper $shouldAddWrapperStyles={!isRenderedInSubMenu}>
                    <DropdownSearchBar
                        placeholder={searchPlaceholder || 'Search...'}
                        value={searchQuery}
                        onChange={updateSearchQuery}
                    />
                </SearchBarWrapper>
            )}
            <ScrollableContent $shouldAddWrapperStyles={!isRenderedInSubMenu}>
                {menu}
                {isLoading && (
                    <LoadingWrapper>
                        <LoadingOutlined />
                    </LoadingWrapper>
                )}
            </ScrollableContent>
        </DropdownCard>
    );
}
