import React from 'react';
import Highlight from 'react-highlighter';
import styled from 'styled-components';
import { useMatchedFieldsByGroup } from '../context/SearchResultContext';
import { useSearchQuery } from '../context/SearchContext';
import { MatchedFieldName } from './constants';
import { useAppConfig } from '../../useAppConfig';

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
    const appConfig = useAppConfig();
    const enableNameHighlight = appConfig.config.visualConfig.searchResult?.enableNameHighlight;
    const matchedFields = useMatchedFieldsByGroup(field);
    const hasMatchedField = !!matchedFields?.length;
    const normalizedSearchQuery = useSearchQuery()?.trim()?.toLowerCase();
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
