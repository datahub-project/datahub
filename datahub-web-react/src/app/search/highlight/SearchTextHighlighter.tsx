import React from 'react';
import Highlight from 'react-highlighter';
import styled from 'styled-components';
import { useMatchedFieldsByName } from '../context/SearchResultContext';
import { useSearchQuery } from '../context/SearchContext';
import { MatchedFieldName } from '../context/constants';

type Props = {
    field: MatchedFieldName;
    text: string;
    enableFullHighlight?: boolean;
};

const HIGHLIGHT_ALL_PATTERN = /.*/;

const StyledHighlight = styled(Highlight).attrs((props) => ({
    matchStyle: { background: props.theme.styles['highlight-color'] },
}))``;

const SearchTextHighlighter = ({ field, text, enableFullHighlight = false }: Props) => {
    const matchedFields = useMatchedFieldsByName(field);
    const hasMatchedField = !!matchedFields?.length;
    const normalizedSearchQuery = useSearchQuery()?.trim().toLowerCase();
    const normalizedText = text.trim().toLowerCase();
    const hasSubstring = hasMatchedField && !!normalizedSearchQuery && normalizedText.includes(normalizedSearchQuery);
    const pattern = enableFullHighlight ? HIGHLIGHT_ALL_PATTERN : undefined;

    return (
        <>
            {hasMatchedField ? (
                <StyledHighlight search={hasSubstring ? normalizedSearchQuery : pattern}>{text}</StyledHighlight>
            ) : (
                text
            )}
        </>
    );
};

export default SearchTextHighlighter;
