import React from 'react';
import { Button, Typography } from 'antd';
import { FilterOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import TabToolbar from '../TabToolbar';
import { SearchBar } from '../../../../../search/SearchBar';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { EntityType, FacetFilterInput, SearchAcrossEntitiesInput } from '../../../../../../types.generated';
import { SearchResultsInterface } from './types';
import SearchExtendedMenu from './SearchExtendedMenu';

const HeaderContainer = styled.div`
    display: flex;
    justify-content: space-between;
    padding-bottom: 16px;
    width: 100%;
`;

const SearchAndDownloadContainer = styled.div`
    display: flex;
`;

const SearchMenuContainer = styled.div`
    margin-top: 7px;
    margin-left: 10px;
`;

type Props = {
    onSearch: (q: string) => void;
    onToggleFilters: () => void;
    placeholderText?: string | null;
    showDownloadCsvButton?: boolean;
    callSearchOnVariables: (variables: {
        input: SearchAcrossEntitiesInput;
    }) => Promise<SearchResultsInterface | null | undefined>;
    entityFilters: EntityType[];
    filters: FacetFilterInput[];
    query: string;
};

export default function EmbeddedListSearchHeader({
    onSearch,
    onToggleFilters,
    placeholderText,
    showDownloadCsvButton,
    callSearchOnVariables,
    entityFilters,
    filters,
    query,
}: Props) {
    const entityRegistry = useEntityRegistry();

    const onQueryChange = (newQuery: string) => {
        onSearch(newQuery);
    };

    return (
        <TabToolbar>
            <HeaderContainer>
                <Button type="text" onClick={onToggleFilters}>
                    <FilterOutlined />
                    <Typography.Text>Filters</Typography.Text>
                </Button>
                <SearchAndDownloadContainer>
                    <SearchBar
                        initialQuery=""
                        placeholderText={placeholderText || 'Search entities...'}
                        suggestions={[]}
                        style={{
                            maxWidth: 220,
                            padding: 0,
                        }}
                        inputStyle={{
                            height: 32,
                            fontSize: 12,
                        }}
                        onSearch={onSearch}
                        onQueryChange={onQueryChange}
                        entityRegistry={entityRegistry}
                    />
                    {/* TODO: in the future, when we add more menu items, we'll show this always */}
                    {showDownloadCsvButton && (
                        <SearchMenuContainer>
                            <SearchExtendedMenu
                                callSearchOnVariables={callSearchOnVariables}
                                entityFilters={entityFilters}
                                filters={filters}
                                query={query}
                            />
                        </SearchMenuContainer>
                    )}
                </SearchAndDownloadContainer>
            </HeaderContainer>
        </TabToolbar>
    );
}
