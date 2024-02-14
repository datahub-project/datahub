import React, { useEffect, useState } from 'react';
import styled, { useTheme } from 'styled-components';
import { debounce } from 'lodash';
import { useHistory } from 'react-router';
import {
    GetAutoCompleteMultipleResultsQuery,
    useGetAutoCompleteMultipleResultsLazyQuery,
} from '../../../graphql/search.generated';
import { navigateToSearchUrl } from '../../searchV2/utils/navigateToSearchUrl';
import analytics, { EventType } from '../../analytics';
import { HALF_SECOND_IN_MS } from '../../entityV2/shared/tabs/Dataset/Queries/utils/constants';
import { EntityType, FacetFilterInput } from '../../../types.generated';
import { useQuickFiltersContext } from '../../../providers/QuickFiltersContext';
import { useAppConfig } from '../../useAppConfig';
import { useUserContext } from '../../context/useUserContext';
import { useEntityRegistry } from '../../useEntityRegistry';
import { getAutoCompleteInputFromQuickFilter } from '../../searchV2/utils/filterUtils';
import { SearchBar } from '../../searchV2/SearchBar';
import { HOME_PAGE_SEARCH_BAR_ID } from '../../onboarding/config/HomePageOnboardingConfig';

const Container = styled.div`
    padding: 10px;
    width: auto;
`;

const autoCompleteStyle = {
    minWidth: 400,
    width: '100%',
    maxWidth: '100%',
    margin: '0px 0px',
    marginBottom: '12px',
    padding: 0,
};

export const SearchHeader = () => {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const [getAutoCompleteResultsForMultiple, { data: suggestionsData }] = useGetAutoCompleteMultipleResultsLazyQuery();
    const userContext = useUserContext();
    const themeConfig = useTheme();
    const appConfig = useAppConfig();
    const [newSuggestionData, setNewSuggestionData] = useState<GetAutoCompleteMultipleResultsQuery | undefined>();
    const { selectedQuickFilter } = useQuickFiltersContext();
    const viewUrn = userContext.localState?.selectedViewUrn;
    const viewsEnabled = appConfig.config?.viewsConfig?.enabled || false;

    useEffect(() => {
        if (suggestionsData !== undefined) {
            setNewSuggestionData(suggestionsData);
        }
    }, [suggestionsData]);

    const onSearch = (query: string, type?: EntityType, filters?: FacetFilterInput[]) => {
        analytics.event({
            type: EventType.HomePageSearchEvent,
            query,
            pageNumber: 1,
            selectedQuickFilterTypes: selectedQuickFilter ? [selectedQuickFilter.field] : undefined,
            selectedQuickFilterValues: selectedQuickFilter ? [selectedQuickFilter.value] : undefined,
        });
        navigateToSearchUrl({
            type,
            query,
            history,
            filters,
        });
    };

    const onAutoComplete = debounce((query: string) => {
        if (query && query.trim() !== '') {
            getAutoCompleteResultsForMultiple({
                variables: {
                    input: {
                        query,
                        limit: 10,
                        viewUrn,
                        ...getAutoCompleteInputFromQuickFilter(selectedQuickFilter),
                    },
                },
            });
        }
    }, HALF_SECOND_IN_MS);

    // const onClickExploreAll = () => {
    //     analytics.event({
    //         type: EventType.HomePageExploreAllClickEvent,
    //     });
    //     navigateToSearchUrl({
    //         query: '*',
    //         history,
    //     });
    // };

    return (
        <Container id={HOME_PAGE_SEARCH_BAR_ID}>
            <SearchBar
                placeholderText={themeConfig.content.search.searchbarMessage}
                suggestions={newSuggestionData?.autoCompleteForMultiple?.suggestions || []}
                onSearch={onSearch}
                onQueryChange={onAutoComplete}
                autoCompleteStyle={autoCompleteStyle}
                entityRegistry={entityRegistry}
                viewsEnabled={viewsEnabled}
                combineSiblings
                showQuickFilters
                showViewAllResults
                showCommandK
                inputStyle={{ backgroundColor: '#ffffff' }}
                style={{ padding: 0 }}
            />
        </Container>
    );
};
