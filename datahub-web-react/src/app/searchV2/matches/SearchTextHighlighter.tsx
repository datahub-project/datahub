import React from 'react';
import Highlight from 'react-highlighter';
import styled from 'styled-components';

import { useSearchQuery } from '@app/search/context/SearchContext';
import { useMatchedFieldsByGroup } from '@app/search/context/SearchResultContext';
import { MatchedFieldName } from '@app/searchV2/matches/constants';
import { useAppConfig } from '@app/useAppConfig';

type Props = {
    field: MatchedFieldName;
    text: string;
    enableFullHighlight?: boolean;
};

const HIGHLIGHT_ALL_PATTERN = /.*/;

const StyledHighlight = styled(Highlight).attrs((props) => ({
    matchStyle: { background: props.theme.colors.bgHighlight },
}))``;

// Quotes around a term are query syntax. An apostrophe inside a word is not.
function highlightQuery(query: string | undefined): string | undefined {
    if (!query) {
        return undefined;
    }
    let normalized = query.trim().toLowerCase();
    let previous = '';
    while (normalized !== previous) {
        previous = normalized;
        normalized = normalized.replace(/(^|\s)['"]+|['"]+(\s|$)/g, '$1$2');
    }
    normalized = normalized.replace(/\s+/g, ' ').trim();
    return normalized || undefined;
}

const SearchTextHighlighter = ({ field, text, enableFullHighlight = false }: Props) => {
    const appConfig = useAppConfig();
    const enableNameHighlight = appConfig.config.visualConfig.searchResult?.enableNameHighlight;
    const matchedFields = useMatchedFieldsByGroup(field);
    const hasMatchedField = !!matchedFields?.length;
    const normalizedSearchQuery = highlightQuery(useSearchQuery());
    const normalizedText = text.trim().toLowerCase();
    const hasSubstring = hasMatchedField && !!normalizedSearchQuery && normalizedText.includes(normalizedSearchQuery);
    const pattern = enableFullHighlight ? HIGHLIGHT_ALL_PATTERN : undefined;

    return (
        <>
            {enableNameHighlight && hasMatchedField ? (
                <StyledHighlight search={hasSubstring ? normalizedSearchQuery : pattern}>{text}</StyledHighlight>
            ) : (
                text
            )}
        </>
    );
};

export default SearchTextHighlighter;
