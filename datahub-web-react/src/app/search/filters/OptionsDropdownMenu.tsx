import { LoadingOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React, { CSSProperties } from 'react';
import styled from 'styled-components/macro';
import { FacetFilterInput, FacetMetadata } from '@src/types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { SearchBar } from '../SearchBar';
import { useEnterKeyListener } from '../../shared/useEnterKeyListener';
import { getIsDateRangeFilter } from './utils';
import DateRangeMenu from './DateRangeMenu/DateRangeMenu';

const StyledButton = styled(Button)`
    width: 100%;
    text-align: center;
    background-color: ${(props) => props.theme.styles['primary-color']};
    color: white;
    border-radius: 0;
`;

export const DropdownMenu = styled.div`
    background-color: white;
    border-radius: 5px;
    box-shadow: 0 3px 6px -4px rgba(0, 0, 0, 0.12), 0 6px 16px 0 rgba(0, 0, 0, 0.08), 0 9px 28px 8px rgba(0, 0, 0, 0.05);
    overflow: hidden;
    min-width: 200px;

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
    searchPlaceholder?: string;
    style?: CSSProperties;
    filter?: FacetMetadata;
    manuallyUpdateFilters?: (newValues: FacetFilterInput[]) => void;
}

export default function OptionsDropdownMenu({
    menu,
    updateFilters,
    isLoading,
    searchQuery,
    updateSearchQuery,
    searchPlaceholder,
    style,
    filter,
    manuallyUpdateFilters,
}: Props) {
    const entityRegistry = useEntityRegistry();

    useEnterKeyListener({ querySelectorToExecuteClick: '#updateFiltersButton' });

    if (filter && manuallyUpdateFilters && getIsDateRangeFilter(filter)) {
        return (
            <DropdownMenu data-testid="filter-dropdown" style={style}>
                <DateRangeMenu field={filter} manuallyUpdateFilters={manuallyUpdateFilters} />
            </DropdownMenu>
        );
    }

    return (
        <DropdownMenu data-testid="filter-dropdown" style={style}>
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
