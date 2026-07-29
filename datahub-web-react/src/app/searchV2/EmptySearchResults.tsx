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

type RefineTarget = 'filtersAndView' | 'filters' | 'view';

// Literal union (not `string`) so the keys stay validated against the `search` namespace once
// typed i18n resources are re-enabled — a typo or a key missing from search.json fails at compile time.
type RefineMessageKey =
    | 'emptyResults.tryRefine.filters'
    | 'emptyResults.tryRefine.filtersAndView'
    | 'emptyResults.tryRefine.view'
    | 'emptyResults.trySuggestion.filters'
    | 'emptyResults.trySuggestion.filtersAndView'
    | 'emptyResults.trySuggestion.view';

function getRefineTarget(filters: FacetFilterInput[], viewUrn?: string | null): RefineTarget | null {
    if (filters.length && viewUrn) return 'filtersAndView';
    if (filters.length) return 'filters';
    if (viewUrn) return 'view';
    return null;
}

// The refine action is part of a full sentence whose word order varies by language, so each
// variant maps to a complete translation key rather than interpolating an English fragment.
function getRefineMessageKey(target: RefineTarget, hasSuggestion: boolean): RefineMessageKey {
    if (hasSuggestion) {
        if (target === 'filtersAndView') return 'emptyResults.trySuggestion.filtersAndView';
        if (target === 'filters') return 'emptyResults.trySuggestion.filters';
        return 'emptyResults.trySuggestion.view';
    }
    if (target === 'filtersAndView') return 'emptyResults.tryRefine.filtersAndView';
    if (target === 'filters') return 'emptyResults.tryRefine.filters';
    return 'emptyResults.tryRefine.view';
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
    const refineTarget = getRefineTarget(filters, viewUrn);

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
            {refineTarget && (
                <Trans
                    t={t}
                    i18nKey={getRefineMessageKey(refineTarget, !!suggestText)}
                    values={{ suggestText }}
                    components={{
                        refineLink: <SuggestedText onClick={clearFiltersAndView} />,
                        suggestLink: <SuggestedText onClick={searchForSuggestion} />,
                    }}
                />
            )}
            {!refineTarget && suggestText && (
                <Trans
                    t={t}
                    i18nKey="emptyResults.didYouMean"
                    values={{ suggestText }}
                    components={{ suggestion: <SuggestedText onClick={searchForSuggestion} /> }}
                />
            )}
            {!refineTarget && !suggestText && (
                <Button onClick={onClickExploreAll}>
                    <RocketOutlined /> {t('emptyResults.exploreAll')}
                </Button>
            )}
        </NoDataContainer>
    );
}
