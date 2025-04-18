import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { useGetSearchResultsForMultipleQuery } from '../../graphql/search.generated';
import { EntityType } from '../../types.generated';
import { ANTD_GRAY } from '../entity/shared/constants';
import { SearchBar } from '../search/SearchBar';
import ClickOutside from '../shared/ClickOutside';
import { useEntityRegistry } from '../useEntityRegistry';
import GloassarySearchResultItem from './GloassarySearchResultItem';

const GlossarySearchWrapper = styled.div`
    position: relative;
`;

const ResultsWrapper = styled.div`
    background-color: white;
    border-radius: 5px;
    box-shadow: 0 3px 6px -4px rgb(0 0 0 / 12%), 0 6px 16px 0 rgb(0 0 0 / 8%), 0 9px 28px 8px rgb(0 0 0 / 5%);
    max-height: 380px;
    overflow: auto;
    padding: 8px;
    position: absolute;
    max-height: 210px;
    overflow: auto;
    width: calc(100% - 24px);
    left: 12px;
    top: 45px;
`;

const TermNodeName = styled.span`
    margin-top: 12px;
    color: ${ANTD_GRAY[8]};
    font-weight: bold;
`;

function GlossarySearch() {
    const [query, setQuery] = useState('');
    const [isSearchBarFocused, setIsSearchBarFocused] = useState(false);
    const entityRegistry = useEntityRegistry();

    const { data } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: [EntityType.GlossaryTerm, EntityType.GlossaryNode],
                query,
                start: 0,
                count: 50,
            },
        },
        skip: !query,
    });

    const searchResults = data?.searchAcrossEntities?.searchResults;

    const renderSearchResults = () => (
        <ResultsWrapper>
            <TermNodeName>Glossary Terms</TermNodeName>
            {searchResults?.map((result) => (
                <GloassarySearchResultItem
                    key={result.entity.urn}
                    entity={result.entity}
                    entityRegistry={entityRegistry}
                    query={query}
                    onResultClick={() => setIsSearchBarFocused(false)}
                />
            ))}
        </ResultsWrapper>
    );

    return (
        <GlossarySearchWrapper>
            <ClickOutside onClickOutside={() => setIsSearchBarFocused(false)}>
                <SearchBar
                    initialQuery={query || ''}
                    placeholderText="Search Glossary"
                    suggestions={[]}
                    hideRecommendations
                    style={{
                        padding: 12,
                        paddingBottom: 5,
                    }}
                    inputStyle={{
                        height: 30,
                        fontSize: 12,
                    }}
                    onSearch={() => null}
                    onQueryChange={(q) => setQuery(q)}
                    entityRegistry={entityRegistry}
                    onFocus={() => setIsSearchBarFocused(true)}
                />
                {isSearchBarFocused && searchResults && !!searchResults.length && renderSearchResults()}
            </ClickOutside>
        </GlossarySearchWrapper>
    );
}

export default GlossarySearch;
