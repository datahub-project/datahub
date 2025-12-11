/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
}

export default function EmptySearchResults({ suggestions }: Props) {
    const { query, filters, viewUrn } = useGetSearchQueryInputs();
    const history = useHistory();
    const userContext = useUserContext();
    const suggestText = suggestions.length > 0 ? suggestions[0].text : '';
    const refineSearchText = getRefineSearchText(filters, viewUrn);

    const onClickExploreAll = useCallback(() => {
        analytics.event({ type: EventType.SearchResultsExploreAllClickEvent });
        navigateToSearchUrl({ query: '*', history });
    }, [history]);

    const searchForSuggestion = () => {
        navigateToSearchUrl({ query: suggestText, history });
    };

    const clearFiltersAndView = () => {
        navigateToSearchUrl({ query, history });
        userContext.updateLocalState({
            ...userContext.localState,
            selectedViewUrn: undefined,
        });
    };

    return (
        <NoDataContainer>
            <Section>No results found for &quot;{query}&quot;</Section>
            {refineSearchText && (
                <>
                    Try <SuggestedText onClick={clearFiltersAndView}>{refineSearchText}</SuggestedText>{' '}
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
                <Button onClick={onClickExploreAll}>
                    <RocketOutlined /> Explore all
                </Button>
            )}
        </NoDataContainer>
    );
}
