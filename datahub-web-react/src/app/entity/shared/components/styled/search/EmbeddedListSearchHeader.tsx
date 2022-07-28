import React from 'react';
import { Button, Typography } from 'antd';
import { CloseCircleOutlined, FilterOutlined } from '@ant-design/icons';
import type { CheckboxValueType } from 'antd/es/checkbox/Group';
import styled from 'styled-components';
import TabToolbar from '../TabToolbar';
import { SearchBar } from '../../../../../search/SearchBar';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { EntityType, FacetFilterInput, SearchAcrossEntitiesInput } from '../../../../../../types.generated';
import { SearchResultsInterface } from './types';
import SearchExtendedMenu from './SearchExtendedMenu';
// import SearchActionMenu from './SearchActionMenu';

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

const SelectedText = styled(Typography.Text)`
    width: 70px;
    top: 5px;
    position: relative;
`;

type Props = {
    onSearch: (q: string) => void;
    onToggleFilters: () => void;
    placeholderText?: string | null;
    callSearchOnVariables: (variables: {
        input: SearchAcrossEntitiesInput;
    }) => Promise<SearchResultsInterface | null | undefined>;
    entityFilters: EntityType[];
    filters: FacetFilterInput[];
    query: string;
    setShowSelectMode: (showSelectMode: boolean) => any;
    showSelectMode: boolean;
    checkedSearchResults: CheckboxValueType[];
};

export default function EmbeddedListSearchHeader({
    onSearch,
    onToggleFilters,
    placeholderText,
    callSearchOnVariables,
    entityFilters,
    filters,
    query,
    setShowSelectMode,
    showSelectMode,
    checkedSearchResults,
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
                    {showSelectMode && <SelectedText>{`${checkedSearchResults.length} selected`}</SelectedText>}
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
                    {showSelectMode ? (
                        <>
                            <Button
                                style={{
                                    marginLeft: '5px',
                                }}
                                onClick={() => setShowSelectMode(false)}
                                type="text"
                            >
                                <CloseCircleOutlined /> Cancel
                            </Button>
                            {/* <SearchMenuContainer>
                                <SearchActionMenu checkedSearchResults={checkedSearchResults} />
                            </SearchMenuContainer> */}
                        </>
                    ) : (
                        <SearchMenuContainer>
                            <SearchExtendedMenu
                                callSearchOnVariables={callSearchOnVariables}
                                entityFilters={entityFilters}
                                filters={filters}
                                query={query}
                                setShowSelectMode={setShowSelectMode}
                            />
                        </SearchMenuContainer>
                    )}
                </SearchAndDownloadContainer>
            </HeaderContainer>
        </TabToolbar>
    );
}
