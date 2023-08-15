import React, { CSSProperties } from 'react';
import Highlight from 'react-highlighter';
import { useMatchedField } from '../context/SearchResultContext';
import { useSearchQuery } from '../context/SearchContext';
import { NormalizedMatchedFieldName } from './constants';

type Props = {
    field?: NormalizedMatchedFieldName;
    text?: string;
};

const HIGHLIGHT_ALL_PATTERN = /.*/;

const highlightStyle: CSSProperties = {
    background: '#F0FFFB',
};

const SearchHighlighter = ({ field, text }: Props) => {
    const matchedField = useMatchedField(field);
    const searchQuery = useSearchQuery();
    const hasSubstring = matchedField?.value && searchQuery && text?.toLowerCase().includes(searchQuery.toLowerCase());

    return (
        <>
            {matchedField ? (
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
