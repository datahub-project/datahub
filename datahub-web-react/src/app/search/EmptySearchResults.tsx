import { RocketOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React, { useCallback } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { ANTD_GRAY_V2 } from '@app/entity/shared/constants';
import { SuggestedText } from '@app/search/suggestions/SearchQuerySugggester';
import useGetSearchQueryInputs from '@app/search/useGetSearchQueryInputs';
import { navigateToSearchUrl } from '@app/search/utils/navigateToSearchUrl';

import { FacetFilterInput, SearchSuggestion } from '@types';

const NoDataContainer = styled.div`
    margin: 40px auto;
    font-size: 16px;
    color: ${ANTD_GRAY_V2[8]};
`;

const Section = styled.div`
    margin-bottom: 16px;
`;

function getRefineSearchText(filters: FacetFilterInput[], viewUrn?: string | null) {
    let text = '';
    if (filters.length && viewUrn) {
        text = 'clearing all filters and selected view';
    } else if (filters.length) {
        text = 'clearing all filters';
    } else if (viewUrn) {
        text = 'clearing the selected view';
    }

    return text;
}

interface Props {
    suggestions: SearchSuggestion[];
    onClickExploreAll?: () => any;
    onClickClearFilters?: () => any;
}

export default function EmptySearchResults({ suggestions, onClickExploreAll, onClickClearFilters }: Props) {
    const { query, filters, viewUrn } = useGetSearchQueryInputs();
    const history = useHistory();
    const userContext = useUserContext();
    const suggestText = suggestions.length > 0 ? suggestions[0].text : '';
    const refineSearchText = getRefineSearchText(filters, viewUrn);

    const handleClickExploreAll = useCallback(() => {
        analytics.event({ type: EventType.SearchResultsExploreAllClickEvent });
        if (onClickExploreAll) onClickExploreAll();
        else navigateToSearchUrl({ query: '*', history });
    }, [history, onClickExploreAll]);

    const searchForSuggestion = () => {
        navigateToSearchUrl({ query: suggestText, history });
    };

    const handleClearFiltersAndView = () => {
        if (onClickClearFilters) {
            onClickClearFilters();
        } else {
            navigateToSearchUrl({ query, history });
            userContext.updateLocalState({
                ...userContext.localState,
                selectedViewUrn: undefined,
            });
        }
    };

    return (
        <NoDataContainer>
            <Section>No results found for &quot;{query}&quot;</Section>
            {refineSearchText && (
                <>
                    Try <SuggestedText onClick={handleClearFiltersAndView}>{refineSearchText}</SuggestedText>{' '}
                    {suggestText && (
                        <>
                            or searching for <SuggestedText onClick={searchForSuggestion}>{suggestText}</SuggestedText>
                        </>
                    )}
                </>
            )}
            {!refineSearchText && suggestText && (
                <>
                    Did you mean <SuggestedText onClick={searchForSuggestion}>{suggestText}</SuggestedText>
                </>
            )}
            {!refineSearchText && !suggestText && (
                <Button onClick={handleClickExploreAll}>
                    <RocketOutlined /> View all
                </Button>
            )}
        </NoDataContainer>
    );
}
