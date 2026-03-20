import { LoadingOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components/macro';

import DropdownSearchBar from '@components/components/Select/private/DropdownSearchBar';

const DropdownCard = styled.div`
    background-color: ${(props) => props.theme.colors.bg};
    border-radius: 12px;
    box-shadow: ${(props) => props.theme.colors.shadowMd};
    overflow: hidden;
    min-width: 200px;
`;

const ScrollableContent = styled.div`
    max-height: 360px;
    overflow: auto;
    min-width: 260px;
    display: flex;
    flex-direction: column;
    gap: 4px;
    padding: 4px;
`;

const SearchBarWrapper = styled.div`
    padding: 8px 8px 4px 8px;
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
}

export default function OptionsDropdownMenu({
    menu,
    isLoading,
    searchQuery,
    updateSearchQuery,
    searchPlaceholder,
    showSearchBar = true,
    className,
}: Props) {
    return (
        <DropdownCard data-testid="filter-dropdown" className={className}>
            {showSearchBar && (
                <SearchBarWrapper>
                    <DropdownSearchBar
                        placeholder={searchPlaceholder || 'Search...'}
                        value={searchQuery}
                        onChange={updateSearchQuery}
                    />
                </SearchBarWrapper>
            )}
            <ScrollableContent>
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
