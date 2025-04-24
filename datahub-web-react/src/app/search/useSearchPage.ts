import { useEffect, useState } from 'react';
import { useHistory } from 'react-router';

import { EntityAndType } from '@app/entity/shared/types';
import { useSelectedSortOption } from '@app/search/context/SearchContext';
import useGetSearchQueryInputs from '@app/search/useGetSearchQueryInputs';
import { UnionType } from '@app/search/utils/constants';
import { navigateToSearchUrl } from '@app/search/utils/navigateToSearchUrl';
import { DownloadSearchResults, DownloadSearchResultsInput } from '@app/search/utils/types';
import { useDownloadScrollAcrossEntitiesSearchResults } from '@app/search/utils/useDownloadScrollAcrossEntitiesSearchResults';
import { scrollToTop } from '@app/shared/searchUtils';
import { PageRoutes } from '@conf/Global';
import { SearchCfg } from '@src/conf';

import { FacetFilterInput, SearchResult } from '@types';

interface Props {
    searchResults: SearchResult[];
    currentPath?: string;
    defaultIsSelectMode?: boolean;
    selectedEntities: EntityAndType[];
    setSelectedEntities: (entities: EntityAndType[]) => void;
    existingSearchParams?: { [key: string]: string };
}

export default function useSearchPage({
    searchResults,
    currentPath = PageRoutes.SEARCH,
    defaultIsSelectMode = false,
    selectedEntities,
    setSelectedEntities,
    existingSearchParams,
}: Props) {
    const selectedSortOption = useSelectedSortOption();
    const history = useHistory();
    const { query, unionType, filters, orFilters, activeType } = useGetSearchQueryInputs();
    const [isSelectMode, setIsSelectMode] = useState(defaultIsSelectMode);

    useEffect(() => {
        if (!defaultIsSelectMode) {
            // When the query changes, then clear the select mode state
            setIsSelectMode(false);
        }
    }, [query, setIsSelectMode, defaultIsSelectMode]);

    useEffect(() => {
        if (!isSelectMode) {
            setSelectedEntities([]);
        }
    }, [isSelectMode, setSelectedEntities]);

    // This hook is simply used to generate a refetch callback that the DownloadAsCsv component can use to
    // download the correct results given the current context.
    // TODO: Use the loading indicator to log a message to the user should download to CSV fail.
    // TODO: Revisit this pattern -- what can we push down?
    const { refetch: refetchForDownload } = useDownloadScrollAcrossEntitiesSearchResults({
        variables: {
            input: {
                types: [],
                query,
                count: SearchCfg.RESULTS_PER_PAGE,
                orFilters,
                scrollId: null,
            },
        },
        skip: true,
    });

    const downloadSearchResults = (
        input: DownloadSearchResultsInput,
    ): Promise<DownloadSearchResults | null | undefined> => {
        return refetchForDownload(input);
    };

    const onChangeFilters = (newFilters: Array<FacetFilterInput>) => {
        navigateToSearchUrl({
            type: activeType,
            query,
            selectedSortOption,
            page: 1,
            filters: newFilters,
            history,
            unionType,
            currentPath,
            existingSearchParams,
        });
    };

    const onChangeUnionType = (newUnionType: UnionType) => {
        navigateToSearchUrl({
            type: activeType,
            query,
            selectedSortOption,
            page: 1,
            filters,
            history,
            unionType: newUnionType,
            currentPath,
            existingSearchParams,
        });
    };

    const onChangePage = (newPage: number) => {
        scrollToTop();
        navigateToSearchUrl({
            type: activeType,
            query,
            selectedSortOption,
            page: newPage,
            filters,
            history,
            unionType,
            currentPath,
            existingSearchParams,
        });
    };

    const onChangeQuery = (newQuery: string) => {
        scrollToTop();
        navigateToSearchUrl({
            type: activeType,
            query: newQuery,
            selectedSortOption,
            page: 1,
            filters,
            history,
            unionType,
            currentPath,
            existingSearchParams,
        });
    };

    const searchResultEntities =
        searchResults?.map((result) => ({
            urn: result.entity.urn,
            type: result.entity.type,
        })) || [];
    const searchResultUrns = searchResultEntities.map((entity) => entity.urn);

    /**
     * Invoked when the "select all" checkbox is clicked.
     *
     * This method either adds the entire current page of search results to
     * the list of selected entities, or removes the current page from the set of selected entities.
     */
    const onChangeSelectAll = (selected: boolean) => {
        if (selected) {
            // Add current page of urns to the master selected entity list
            const entitiesToAdd = searchResultEntities.filter(
                (entity) =>
                    selectedEntities.findIndex(
                        (element) => element.urn === entity.urn && element.type === entity.type,
                    ) < 0,
            );
            setSelectedEntities(Array.from(new Set(selectedEntities.concat(entitiesToAdd))));
        } else {
            // Filter out the current page of entity urns from the list
            setSelectedEntities(selectedEntities.filter((entity) => searchResultUrns.indexOf(entity.urn) === -1));
        }
    };

    return {
        selectedEntities,
        setSelectedEntities,
        isSelectMode,
        setIsSelectMode,
        downloadSearchResults,
        onChangeFilters,
        onChangeUnionType,
        onChangePage,
        onChangeSelectAll,
        onChangeQuery,
    };
}
