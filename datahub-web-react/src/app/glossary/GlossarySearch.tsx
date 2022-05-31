import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';
import { useGetSearchResultsForMultipleQuery } from '../../graphql/search.generated';
import { EntityType } from '../../types.generated';
import { IconStyleType } from '../entity/Entity';
import { ANTD_GRAY } from '../entity/shared/constants';
import { SearchBar } from '../search/SearchBar';
import ClickOutside from '../shared/ClickOutside';
import { useEntityRegistry } from '../useEntityRegistry';

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

const SearchResult = styled(Link)`
    color: #262626;
    display: inline-block;
    height: 100%;
    padding: 6px 8px;
    width: 100%;
    &:hover {
        background-color: ${ANTD_GRAY[3]};
        color: #262626;
    }
`;

const IconWrapper = styled.span`
    margin-right: 8px;
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

    return (
        <GlossarySearchWrapper>
            <ClickOutside onClickOutside={() => setIsSearchBarFocused(false)}>
                <SearchBar
                    initialQuery={query || ''}
                    placeholderText="Search Glossary"
                    suggestions={[]}
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
                {isSearchBarFocused && searchResults && !!searchResults.length && (
                    <ResultsWrapper>
                        {searchResults.map((result) => {
                            return (
                                <SearchResult
                                    to={`/${entityRegistry.getPathName(result.entity.type)}/${result.entity.urn}`}
                                    onClick={() => setIsSearchBarFocused(false)}
                                >
                                    <IconWrapper>
                                        {entityRegistry.getIcon(result.entity.type, 12, IconStyleType.ACCENT)}
                                    </IconWrapper>
                                    {entityRegistry.getDisplayName(result.entity.type, result.entity)}
                                </SearchResult>
                            );
                        })}
                    </ResultsWrapper>
                )}
            </ClickOutside>
        </GlossarySearchWrapper>
    );
}

export default GlossarySearch;
