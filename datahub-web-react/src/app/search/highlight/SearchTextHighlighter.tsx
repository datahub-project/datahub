import React from 'react';
import Highlight from 'react-highlighter';
import styled from 'styled-components';
import { useMatchedFieldsByNormalizedFieldName } from '../context/SearchResultContext';
import { useSearchQuery } from '../context/SearchContext';
import { NormalizedMatchedFieldName } from '../context/constants';

type Props = {
    field: NormalizedMatchedFieldName;
    text: string;
};

const HIGHLIGHT_ALL_PATTERN = /.*/;

const StyledHighlight = styled(Highlight).attrs((props) => ({
    matchStyle: { background: props.theme.styles['highlight-color'] },
}))``;

const SearchTextHighlighter = ({ field, text }: Props) => {
    const matchedField = useMatchedFieldsByNormalizedFieldName(field);
    const hasMatchedField = !!matchedField?.length;
    const normalizedSearchQuery = useSearchQuery()?.trim().toLowerCase();
    const normalizedText = text.trim().toLowerCase();
    const hasSubstring = hasMatchedField && !!normalizedSearchQuery && normalizedText.includes(normalizedSearchQuery);

    return (
        <>
            {hasMatchedField ? (
                <StyledHighlight search={hasSubstring ? normalizedSearchQuery : HIGHLIGHT_ALL_PATTERN}>
                    {text}
                </StyledHighlight>
            ) : (
                text
            )}
        </>
    );
};

export default SearchTextHighlighter;
