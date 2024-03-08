import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';
import { useGetSearchResultsForMultipleQuery } from '../../graphql/search.generated';
import { EntityType } from '../../types.generated';
import { IconStyleType } from '../entityV2/Entity';
import { ANTD_GRAY, REDESIGN_COLORS } from '../entityV2/shared/constants';
import { SearchBar } from '../searchV2/SearchBar';
import ClickOutside from '../shared/ClickOutside';
import { useEntityRegistry } from '../useEntityRegistry';

const GlossarySearchWrapper = styled.div`
    position: relative;
    border-bottom: 1px solid ${REDESIGN_COLORS.BORDER_3};
`;

const ResultsWrapper = styled.div`
    background-color: white;
    border-radius: 5px;
    box-shadow: 0 3px 6px -4px rgb(0 0 0 / 12%), 0 6px 16px 0 rgb(0 0 0 / 8%), 0 9px 28px 8px rgb(0 0 0 / 5%);
    padding: 8px;
    position: absolute;
    max-height: 210px;
    overflow: auto;
    width: calc(100% - 24px);
    left: 12px;
    top: 45px;
    z-index: 1;
`;

const SearchResult = styled(Link)`
    color: ${ANTD_GRAY[11]};
    display: inline-block;
    height: 100%;
    padding: 6px 8px;
    width: 100%;
    &:hover {
        background-color: ${ANTD_GRAY[3]};
        color: ${ANTD_GRAY[11]};
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
                    placeholderText="Search"
                    suggestions={[]}
                    hideRecommendations
                    style={{
                        padding: 9,
                        paddingLeft: 9,
                        paddingBottom: 14,
                    }}
                    inputStyle={{
                        height: 30,
                        fontSize: 10,
                        fontWeight: 500,
                        backgroundColor: ANTD_GRAY[3],
                    }}
                    textColor={ANTD_GRAY[10]}
                    placeholderColor={REDESIGN_COLORS.PLACEHOLDER_PURPLE}
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
                                    to={`${entityRegistry.getEntityUrl(result.entity.type, result.entity.urn)}`}
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
