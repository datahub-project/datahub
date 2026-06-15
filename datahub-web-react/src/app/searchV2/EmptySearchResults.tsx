import { RocketOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React, { useCallback } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { SuggestedText } from '@app/searchV2/suggestions/SearchQuerySugggester';
import useGetSearchQueryInputs from '@app/searchV2/useGetSearchQueryInputs';
import { navigateToSearchUrl } from '@app/searchV2/utils/navigateToSearchUrl';

import { FacetFilterInput, SearchSuggestion } from '@types';

const NoDataContainer = styled.div`
    margin: 40px auto;
    font-size: 16px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const Section = styled.div`
    margin-bottom: 16px;
`;

// Each branch maps to a complete translation key so the whole sentence — including the
// refine-link wording — is translated as one unit, rather than interpolating an English fragment.
function getRefineSuggestionKey(filters: FacetFilterInput[], viewUrn?: string | null): string | null {
    if (filters.length && viewUrn) {
        return 'emptyResults.trySuggestionClearFiltersAndView';
    }
    if (filters.length) {
        return 'emptyResults.trySuggestionClearFilters';
    }
    if (viewUrn) {
        return 'emptyResults.trySuggestionClearView';
    }
    return null;
}

interface Props {
    suggestions: SearchSuggestion[];
}

export default function EmptySearchResults({ suggestions }: Props) {
    const { t } = useTranslation('search');
    const { query, filters, viewUrn } = useGetSearchQueryInputs();
    const history = useHistory();
    const userContext = useUserContext();
    const suggestText = suggestions.length > 0 ? suggestions[0].text : '';
    const refineSuggestionKey = getRefineSuggestionKey(filters, viewUrn);

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
            <Section>{t('emptyResults.noResultsFound', { query })}</Section>
            {refineSuggestionKey && (
                <Trans
                    t={t}
                    i18nKey={refineSuggestionKey}
                    values={{ suggestText }}
                    components={{
                        refineLink: <SuggestedText onClick={clearFiltersAndView} />,
                        suggestLink: <SuggestedText onClick={searchForSuggestion} />,
                    }}
                />
            )}
            {!refineSuggestionKey && suggestText && (
                <Trans
                    t={t}
                    i18nKey="emptyResults.didYouMean"
                    values={{ suggestText }}
                    components={{ suggestion: <SuggestedText onClick={searchForSuggestion} /> }}
                />
            )}
            {!refineSuggestionKey && !suggestText && (
                <Button onClick={onClickExploreAll}>
                    <RocketOutlined /> {t('emptyResults.exploreAll')}
                </Button>
            )}
        </NoDataContainer>
    );
}
