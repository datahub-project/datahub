import { LoadingOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';
import styled, { useTheme } from 'styled-components/macro';

import { SearchBar } from '@app/searchV2/SearchBar';
import { useEnterKeyListener } from '@app/shared/useEnterKeyListener';
import { useEntityRegistry } from '@app/useEntityRegistry';

const StyledButton = styled(Button)`
    width: 100%;
    text-align: center;
    background-color: ${(p) => p.theme.colors.buttonFillBrand};
    color: ${(props) => props.theme.colors.textOnFillBrand};
    border-radius: 0;
`;

export const DropdownMenu = styled.div<{ type: 'card' | 'default' }>`
    background-color: ${(props) => props.theme.colors.bgSurface};
    ${(props) => props.type === 'card' && 'border-radius: 5px;'}
    ${(props) => props.type === 'card' && `box-shadow: ${props.theme.colors.shadowMd};`}
    overflow: hidden;
    min-width: 200px;

    .ant-dropdown-menu-title-content {
        background-color: ${(props) => props.theme.colors.bgSurface};
        &:hover {
            background-color: ${(props) => props.theme.colors.bgSurface};
        }
    }
    .ant-dropdown-menu {
        padding: 7px;
    }
`;

const ScrollableContent = styled.div`
    max-height: 380px;
    overflow: auto;
    min-width: 260px;
`;

const LoadingWrapper = styled.div`
    display: flex;
    justify-content: center;
    padding: 9px;

    svg {
        height: 16px;
        width: 16px;
    }
`;

interface Props {
    menu: React.ReactNode;
    updateFilters: () => void;
    isLoading?: boolean;
    searchQuery?: string;
    updateSearchQuery?: (query: string) => void;
    searchPlaceholder?: string;
    showSearchBar?: boolean;
    type?: 'card' | 'default';
    className?: string;
}

export default function OptionsDropdownMenu({
    menu,
    updateFilters,
    isLoading,
    searchQuery,
    updateSearchQuery,
    searchPlaceholder,
    showSearchBar = true,
    type = 'card',
    className,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const theme = useTheme();

    useEnterKeyListener({ querySelectorToExecuteClick: '#updateFiltersButton' });

    return (
        <DropdownMenu type={type} data-testid="filter-dropdown" className={className}>
            <ScrollableContent>
                {showSearchBar && (
                    <SearchBar
                        initialQuery={searchQuery}
                        placeholderText={searchPlaceholder || ''}
                        suggestions={[]}
                        hideRecommendations
                        style={{
                            padding: 12,
                            paddingBottom: 5,
                        }}
                        inputStyle={{
                            height: 30,
                            fontSize: 12,
                            borderRadius: 8,
                        }}
                        onSearch={() => null}
                        onQueryChange={updateSearchQuery}
                        entityRegistry={entityRegistry}
                        textColor={theme.colors.text}
                        placeholderColor={theme.colors.textDisabled}
                    />
                )}
                {React.cloneElement(menu as React.ReactElement, { style: { boxShadow: 'none' } })}
                {isLoading && (
                    <LoadingWrapper>
                        <LoadingOutlined />
                    </LoadingWrapper>
                )}
            </ScrollableContent>
            <StyledButton id="updateFiltersButton" type="text" onClick={updateFilters} data-testid="update-filters">
                Update
            </StyledButton>
        </DropdownMenu>
    );
}
