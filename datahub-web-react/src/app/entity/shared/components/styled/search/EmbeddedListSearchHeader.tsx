import { FilterOutlined } from '@ant-design/icons';
import { Button, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import TabToolbar from '@app/entity/shared/components/styled/TabToolbar';
import SearchExtendedMenu from '@app/entity/shared/components/styled/search/SearchExtendedMenu';
import { SearchSelectBar } from '@app/entity/shared/components/styled/search/SearchSelectBar';
import { EntityAndType } from '@app/entity/shared/types';
import { SearchBar } from '@app/search/SearchBar';
import { DownloadSearchResults, DownloadSearchResultsInput } from '@app/search/utils/types';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { useSearchContext } from '@src/app/search/context/SearchContext';
import SearchSortSelect from '@src/app/search/sorting/SearchSortSelect';

import { AndFilterInput } from '@types';

const HeaderContainer = styled.div`
    display: flex;
    justify-content: space-between;
    width: 100%;
    padding-right: 4px;
    padding-left: 4px;
`;

const SearchAndDownloadContainer = styled.div`
    display: flex;
    align-items: center;
`;

type Props = {
    onSearch: (q: string) => void;
    onToggleFilters: () => void;
    placeholderText?: string | null;
    downloadSearchResults: (input: DownloadSearchResultsInput) => Promise<DownloadSearchResults | null | undefined>;
    filters: AndFilterInput[];
    query: string;
    isSelectMode: boolean;
    isSelectAll: boolean;
    selectedEntities: EntityAndType[];
    setSelectedEntities: (entities: EntityAndType[]) => void;
    setIsSelectMode: (showSelectMode: boolean) => any;
    onChangeSelectAll: (selected: boolean) => void;
    refetch?: () => void;
    searchBarStyle?: any;
    searchBarInputStyle?: any;
};

export default function EmbeddedListSearchHeader({
    onSearch,
    onToggleFilters,
    placeholderText,
    downloadSearchResults,
    filters,
    query,
    isSelectMode,
    isSelectAll,
    selectedEntities,
    setSelectedEntities,
    setIsSelectMode,
    onChangeSelectAll,
    refetch,
    searchBarStyle,
    searchBarInputStyle,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const { selectedSortOption, setSelectedSortOption } = useSearchContext();

    return (
        <>
            <TabToolbar>
                <HeaderContainer>
                    <Button type="text" onClick={onToggleFilters}>
                        <FilterOutlined />
                        <Typography.Text>Filters</Typography.Text>
                    </Button>
                    <SearchAndDownloadContainer>
                        <SearchBar
                            dataTestId="embedded-search-bar"
                            initialQuery=""
                            placeholderText={placeholderText || 'Search entities...'}
                            suggestions={[]}
                            style={
                                searchBarStyle || {
                                    maxWidth: 220,
                                    padding: 0,
                                }
                            }
                            inputStyle={
                                searchBarInputStyle || {
                                    height: 32,
                                    fontSize: 12,
                                }
                            }
                            onSearch={onSearch}
                            onQueryChange={onSearch}
                            entityRegistry={entityRegistry}
                            hideRecommendations
                        />
                        <SearchSortSelect
                            selectedSortOption={selectedSortOption}
                            setSelectedSortOption={setSelectedSortOption}
                        />
                        <SearchExtendedMenu
                            downloadSearchResults={downloadSearchResults}
                            filters={filters}
                            query={query}
                            setShowSelectMode={setIsSelectMode}
                        />
                    </SearchAndDownloadContainer>
                </HeaderContainer>
            </TabToolbar>
            {isSelectMode && (
                <TabToolbar>
                    <SearchSelectBar
                        isSelectAll={isSelectAll}
                        onChangeSelectAll={onChangeSelectAll}
                        selectedEntities={selectedEntities}
                        setSelectedEntities={setSelectedEntities}
                        onCancel={() => {
                            setIsSelectMode(false);
                        }}
                        refetch={refetch}
                    />
                </TabToolbar>
            )}
        </>
    );
}
