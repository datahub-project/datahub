import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import { navigateToSearchUrl } from '@app/searchV2/utils/navigateToSearchUrl';

import { SearchSuggestion } from '@types';

const TextWrapper = styled.div`
    font-size: 14px;
    color: ${(props) => props.theme.colors.textSecondary};
    margin: 8px 0px 0px 12px;
`;

export const SuggestedText = styled.span`
    color: ${(props) => props.theme.colors.textBrand};
    text-decoration: underline ${(props) => props.theme.colors.textBrand};
    cursor: pointer;
`;

interface Props {
    suggestions: SearchSuggestion[];
}

export default function SearchQuerySuggester({ suggestions }: Props) {
    const { t } = useTranslation('search');
    const history = useHistory();

    if (suggestions.length === 0) return null;
    const suggestText = suggestions[0].text;

    function searchForSuggestion() {
        navigateToSearchUrl({ query: suggestText, history });
    }

    return (
        <TextWrapper>
            <Trans
                t={t}
                i18nKey="suggestions.didYouMean"
                values={{ suggestText }}
                components={{ suggestion: <SuggestedText onClick={searchForSuggestion} /> }}
            />
        </TextWrapper>
    );
}
