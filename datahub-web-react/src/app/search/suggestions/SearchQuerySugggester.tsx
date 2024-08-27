import styled from 'styled-components';
import React from 'react';
import { useHistory } from 'react-router';
import { useTranslation } from 'react-i18next';
import { SearchSuggestion } from '../../../types.generated';
import { navigateToSearchUrl } from '../utils/navigateToSearchUrl';
import { ANTD_GRAY_V2 } from '../../entity/shared/constants';

const TextWrapper = styled.div`
    font-size: 14px;
    color: ${ANTD_GRAY_V2[8]};
    margin: 16px 0 -8px 32px;
`;

export const SuggestedText = styled.span`
    color: ${(props) => props.theme.styles['primary-color']};
    text-decoration: underline ${(props) => props.theme.styles['primary-color']};
    cursor: pointer;
`;

interface Props {
    suggestions: SearchSuggestion[];
}

export default function SearchQuerySuggester({ suggestions }: Props) {
    const { t } = useTranslation();
    const history = useHistory();

    if (suggestions.length === 0) return null;
    const suggestText = suggestions[0].text;

    function searchForSuggestion() {
        navigateToSearchUrl({ query: suggestText, history });
    }

    return (
        <TextWrapper>
            {t('search.didYouMean_component')}{' '}
            <SuggestedText onClick={searchForSuggestion}>{suggestText}</SuggestedText>
        </TextWrapper>
    );
}
