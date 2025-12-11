/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { colors } from '@components';
import React from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import { navigateToSearchUrl } from '@app/searchV2/utils/navigateToSearchUrl';

import { SearchSuggestion } from '@types';

const TextWrapper = styled.div`
    font-size: 14px;
    color: ${colors.gray[1700]};
    margin: 8px 0px 0px 12px;
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
