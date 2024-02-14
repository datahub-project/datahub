import styled from 'styled-components';
import React from 'react';
import { useHistory } from 'react-router';
import { SearchSuggestion } from '../../../types.generated';
import { navigateToSearchUrl } from '../utils/navigateToSearchUrl';
import { ANTD_GRAY_V2 } from '../../entity/shared/constants';

const TextWrapper = styled.div`
    font-size: 14px;
    color: ${ANTD_GRAY_V2[8]};
    margin: -8px 0px 16px 24px;
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
    const history = useHistory();

    if (suggestions.length === 0) return null;
    const suggestText = suggestions[0].text;

    function searchForSuggestion() {
        navigateToSearchUrl({ query: suggestText, history });
    }

    return (
        <TextWrapper>
            Did you mean <SuggestedText onClick={searchForSuggestion}>{suggestText}</SuggestedText>?
        </TextWrapper>
    );
}
