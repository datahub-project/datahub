import React, { useContext, useState } from 'react';
import { Button as AntButton, Typography } from 'antd';
import { ExclamationCircleFilled, FilterOutlined } from '@ant-design/icons';
import InfoPopover from '@src/app/sharedV2/icons/InfoPopover';
import { Button, colors } from '@src/alchemy-components';
import styled from 'styled-components/macro';
import SearchSortSelect from '@src/app/searchV2/sorting/SearchSortSelect';
import { useSearchContext } from '@src/app/search/context/SearchContext';
import TabToolbar from '../TabToolbar';
import { SearchBar } from '../../../../../search/SearchBar';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { AndFilterInput, LineageSearchPath } from '../../../../../../types.generated';
import { SearchSelectBar } from './SearchSelectBar';
import { EntityAndType } from '../../../../../entity/shared/types';
import { DownloadSearchResultsInput, DownloadSearchResults } from '../../../../../search/utils/types';
import SearchMenuItems from '../../../../../sharedV2/search/SearchMenuItems';
import { LineageTabContext } from '../../../tabs/Lineage/LineageTabContext';

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
    display: flex;
`;

const ImpactAnalysisWarning = styled.div`
    gap: 8px;
    padding: 12px 20px;
    display: flex;
    align-items: center;
    background-color: ${colors.yellow[0]};
    z-index: 1;
`;

const StyledButton = styled(Button)`
    margin-left: auto;
    color: #ee9521;
    padding: 0;
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
    numResults?: number;
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
    numResults,
    searchBarStyle,
    searchBarInputStyle,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const { selectedSortOption, setSelectedSortOption } = useSearchContext();
    const { lineageSearchPath } = useContext(LineageTabContext);
    const [showLightningWarning, setShowLightningWarning] = useState(true);

    return (
        <>
            <TabToolbar>
                <HeaderContainer>
                    <AntButton type="text" onClick={onToggleFilters}>
                        <FilterOutlined />
                        <Typography.Text>Filters</Typography.Text>
                    </AntButton>
                    <SearchAndDownloadContainer>
                        <SearchBar
                            data-testid="embedded-search-bar"
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
                        <SearchMenuContainer>
                            <SearchSortSelect
                                selectedSortOption={selectedSortOption}
                                setSelectedSortOption={setSelectedSortOption}
                            />
                            <SearchMenuItems
                                downloadSearchResults={downloadSearchResults}
                                filters={filters}
                                query={query}
                                setShowSelectMode={setIsSelectMode}
                                totalResults={numResults}
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
                        setSelectedEntities={setSelectedEntities}
                        onCancel={() => {
                            setIsSelectMode(false);
                        }}
                        refetch={refetch}
                    />
                </TabToolbar>
            )}
            {showLightningWarning && lineageSearchPath === LineageSearchPath.Lightning && (
                <ImpactAnalysisWarning data-testid="lightning-cache-warning">
                    <ExclamationCircleFilled style={{ color: colors.yellow[1000], fontSize: 16 }} />
                    Using lineage search cache, results may be unexpected
                    <InfoPopover
                        iconColor="rgba(0, 0, 0, 0.85)"
                        content={
                            <>
                                When there are enough results, we use a lineage search cache which may return
                                <br />
                                assets that do not exist in your DataHub instance
                            </>
                        }
                    />
                    <StyledButton
                        onClick={() => setShowLightningWarning(false)}
                        variant="text"
                        icon="Close"
                        size="xl"
                        data-testid="close-lightning-cache-warning"
                    />
                </ImpactAnalysisWarning>
            )}
        </>
    );
}
