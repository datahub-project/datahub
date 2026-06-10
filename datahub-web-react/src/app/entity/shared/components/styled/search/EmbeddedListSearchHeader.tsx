import { FilterOutlined } from '@ant-design/icons';
import { Button, Typography } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import TabToolbar from '@app/entity/shared/components/styled/TabToolbar';
import SearchExtendedMenu from '@app/entity/shared/components/styled/search/SearchExtendedMenu';
import { SearchSelectBar } from '@app/entity/shared/components/styled/search/SearchSelectBar';
import { EntityAndType } from '@app/entity/shared/types';
import { SearchBar } from '@app/search/SearchBar';
import { DownloadSearchResults, DownloadSearchResultsInput } from '@app/search/utils/types';
import { useEntityRegistry } from '@app/useEntityRegistry';

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

const SearchMenuContainer = styled.div`
    margin-left: 10px;
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
    setIsSelectMode,
    onChangeSelectAll,
    refetch,
    searchBarStyle,
    searchBarInputStyle,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const { t } = useTranslation('entityV1.shared.components');

    return (
        <>
            <TabToolbar>
                <HeaderContainer>
                    <Button type="text" onClick={onToggleFilters}>
                        <FilterOutlined />
                        <Typography.Text>{t('embeddedListSearch.filters')}</Typography.Text>
                    </Button>
                    <SearchAndDownloadContainer>
                        <SearchBar
                            data-testid="embedded-search-bar"
                            initialQuery=""
                            placeholderText={placeholderText || t('embeddedListSearch.searchPlaceholder')}
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
                        <SearchMenuContainer>
                            <SearchExtendedMenu
                                downloadSearchResults={downloadSearchResults}
                                filters={filters}
                                query={query}
                                setShowSelectMode={setIsSelectMode}
                            />
                        </SearchMenuContainer>
                    </SearchAndDownloadContainer>
                </HeaderContainer>
            </TabToolbar>
            {isSelectMode && (
                <TabToolbar>
                    <SearchSelectBar
                        isSelectAll={isSelectAll}
                        onChangeSelectAll={onChangeSelectAll}
                        selectedEntities={selectedEntities}
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
