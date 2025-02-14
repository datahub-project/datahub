import { LoadingOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { ANTD_GRAY, SEARCH_COLORS } from '../../entityV2/shared/constants';
import { useEnterKeyListener } from '../../shared/useEnterKeyListener';
import { useEntityRegistry } from '../../useEntityRegistry';
import { SearchBar } from '../SearchBar';

const StyledButton = styled(Button)`
    width: 100%;
    text-align: center;
    background-color: ${SEARCH_COLORS.TITLE_PURPLE};
    color: white;
    border-radius: 0;
`;

export const DropdownMenu = styled.div<{ type: 'card' | 'default' }>`
    background-color: white;
    ${(props) => props.type === 'card' && 'border-radius: 5px;'}
    ${(props) =>
        props.type === 'card' &&
        'box-shadow: 0 3px 6px -4px rgba(0, 0, 0, 0.12), 0 6px 16px 0 rgba(0, 0, 0, 0.08), 0 9px 28px 8px rgba(0, 0, 0, 0.05);'}
    overflow: hidden;
    min-width: 200px;

    .ant-dropdown-menu-title-content {
        background-color: white;
        &:hover {
            background-color: white;
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
                        textColor={ANTD_GRAY[9]}
                        placeholderColor={ANTD_GRAY[6]}
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
