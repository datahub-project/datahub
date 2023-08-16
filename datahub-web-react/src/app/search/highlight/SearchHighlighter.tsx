import React from 'react';
import Highlight from 'react-highlighter';
import styled from 'styled-components';
import { useMatchedFieldsByNormalizedFieldName } from '../context/SearchResultContext';
import { useSearchQuery } from '../context/SearchContext';
import { NormalizedMatchedFieldName } from '../context/constants';

type Props = {
    field: NormalizedMatchedFieldName;
    text: string;
    highlightAllOnFallback?: boolean;
};

const HIGHLIGHT_ALL_PATTERN = /.*/;

const StyledHighlight = styled(Highlight).attrs((props) => ({
    matchStyle: { background: props.theme.styles['highlight-color'] },
}))``;

const SearchHighlighter = ({ field, text, highlightAllOnFallback = true }: Props) => {
    const matchedField = useMatchedFieldsByNormalizedFieldName(field);
    const hasMatchedField = !!matchedField?.length;
    const searchQuery = useSearchQuery();
    const hasSubstring = hasMatchedField && !!searchQuery && text.toLowerCase().includes(searchQuery.toLowerCase());
    const pattern = highlightAllOnFallback ? HIGHLIGHT_ALL_PATTERN : undefined;

    return (
        <>
            {hasMatchedField ? (
                <StyledHighlight search={hasSubstring ? searchQuery : pattern}>{text}</StyledHighlight>
            ) : (
                text
            )}
        </>
    );
};

export default SearchHighlighter;
