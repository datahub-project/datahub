import React, { CSSProperties } from 'react';
import Highlight from 'react-highlighter';
import { HighlightField, useHighlightedValue } from './HighlightContext';
import { useSearchQuery } from '../context/SearchContext';

type Props = {
    field?: HighlightField;
    text?: string;
};

const HIGHLIGHT_ALL_PATTERN = /.*/;

const highlightStyle: CSSProperties = {
    background: '#F0FFFB',
};

const SearchHighlighter = ({ field, text }: Props) => {
    const highlightedValue = useHighlightedValue(field);
    const searchQuery = useSearchQuery();
    const hasSubstring = searchQuery && text?.includes(searchQuery);

    return (
        <>
            {highlightedValue ? (
                <Highlight search={hasSubstring ? searchQuery : HIGHLIGHT_ALL_PATTERN} matchStyle={highlightStyle}>
                    {text}
                </Highlight>
            ) : (
                text
            )}
        </>
    );
};

export default SearchHighlighter;
