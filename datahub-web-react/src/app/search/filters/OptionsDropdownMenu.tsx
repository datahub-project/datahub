import { LoadingOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { useEntityRegistry } from '../../useEntityRegistry';
import { SearchBar } from '../SearchBar';
import { useEnterKeyListener } from '../../shared/useEnterKeyListener';

const StyledButton = styled(Button)`
    width: 100%;
    text-align: center;
    background-color: ${(props) => props.theme.styles['primary-color']};
    color: white;
    border-radius: 0;
`;

export const DropdownMenu = styled.div<{ alignRight?: boolean }>`
    background-color: white;
    border-radius: 5px;
    box-shadow: 0 3px 6px -4px rgba(0, 0, 0, 0.12), 0 6px 16px 0 rgba(0, 0, 0, 0.08), 0 9px 28px 8px rgba(0, 0, 0, 0.05);
    overflow: hidden;
    min-width: 200px;

    ${(props) =>
        props.alignRight &&
        `
    position: absolute;
    left: 205px;
    top: -34px;
    `}

    .ant-dropdown-menu-title-content {
        background-color: white;
        &:hover {
            background-color: white;
        }
    }
`;

const ScrollableContent = styled.div`
    max-height: 312px;
    overflow: auto;
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
    isLoading: boolean;
    searchQuery: string;
    updateSearchQuery: (query: string) => void;
    alignRight?: boolean;
    searchPlaceholder?: string;
}

export default function OptionsDropdownMenu({
    menu,
    updateFilters,
    isLoading,
    searchQuery,
    updateSearchQuery,
    alignRight,
    searchPlaceholder,
}: Props) {
    const entityRegistry = useEntityRegistry();

    useEnterKeyListener({ querySelectorToExecuteClick: '#updateFiltersButton' });

    return (
        <DropdownMenu alignRight={alignRight} data-testid="filter-dropdown">
            <ScrollableContent>
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
                    onQueryChange={(q) => updateSearchQuery(q)}
                    entityRegistry={entityRegistry}
                />
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
