import React, { CSSProperties, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';
import Highlight from 'react-highlighter';
import { useGetSearchResultsForMultipleQuery } from '../../graphql/search.generated';
import { EntityType } from '../../types.generated';
import { IconStyleType } from '../entity/Entity';
import { ANTD_GRAY } from '../entity/shared/constants';
import { SearchBar } from '../search/SearchBar';
import ClickOutside from '../shared/ClickOutside';
import { useEntityRegistry } from '../useEntityRegistry';
import DomainIcon from './DomainIcon';
import ParentEntities from '../search/filters/ParentEntities';
import { getParentDomains } from './utils';

const DomainSearchWrapper = styled.div`
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
    z-index: 1;
`;

const SearchResult = styled(Link)`
    color: #262626;
    display: flex;
    align-items: center;
    gap: 8px;
    height: 100%;
    padding: 6px 8px;
    width: 100%;
    &:hover {
        background-color: ${ANTD_GRAY[3]};
        color: #262626;
    }
`;

const IconWrapper = styled.span``;

const highlightMatchStyle: CSSProperties = {
    fontWeight: 'bold',
    background: 'none',
    padding: 0,
};

function DomainSearch() {
    const [query, setQuery] = useState('');
    const [isSearchBarFocused, setIsSearchBarFocused] = useState(false);
    const entityRegistry = useEntityRegistry();

    const { data } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: [EntityType.Domain],
                query,
                start: 0,
                count: 50,
            },
        },
        skip: !query,
    });

    const searchResults = data?.searchAcrossEntities?.searchResults;
    const timerRef = useRef(-1);
    const handleQueryChange = (q: string) => {
        window.clearTimeout(timerRef.current);
        timerRef.current = window.setTimeout(() => {
            setQuery(q);
        }, 250);
    };

    return (
        <DomainSearchWrapper>
            <ClickOutside onClickOutside={() => setIsSearchBarFocused(false)}>
                <SearchBar
                    initialQuery={query || ''}
                    placeholderText="Search Domains"
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
                    onQueryChange={(q) => handleQueryChange(q)}
                    entityRegistry={entityRegistry}
                    onFocus={() => setIsSearchBarFocused(true)}
                />
                {isSearchBarFocused && searchResults && !!searchResults.length && (
                    <ResultsWrapper>
                        {searchResults.map((result) => {
                            return (
                                <SearchResult
                                    to={entityRegistry.getEntityUrl(result.entity.type, result.entity.urn)}
                                    onClick={() => setIsSearchBarFocused(false)}
                                >
                                    <IconWrapper>
                                        {result.entity.type === EntityType.Domain ? (
                                            <DomainIcon
                                                style={{
                                                    fontSize: 16,
                                                    color: '#BFBFBF',
                                                }}
                                            />
                                        ) : (
                                            entityRegistry.getIcon(result.entity.type, 12, IconStyleType.ACCENT)
                                        )}
                                    </IconWrapper>
                                    <div>
                                        <ParentEntities
                                            parentEntities={getParentDomains(result.entity, entityRegistry)}
                                        />
                                        <Highlight matchStyle={highlightMatchStyle} search={query}>
                                            {entityRegistry.getDisplayName(result.entity.type, result.entity)}
                                        </Highlight>
                                    </div>
                                </SearchResult>
                            );
                        })}
                    </ResultsWrapper>
                )}
            </ClickOutside>
        </DomainSearchWrapper>
    );
}

export default DomainSearch;
