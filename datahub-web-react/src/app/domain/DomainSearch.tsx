import React, { useRef, useState } from 'react';
import { LoadingOutlined } from '@ant-design/icons';
import styled from 'styled-components/macro';
import { useGetSearchResultsForMultipleQuery } from '../../graphql/search.generated';
import { EntityType } from '../../types.generated';
import { SearchBar } from '../search/SearchBar';
import ClickOutside from '../shared/ClickOutside';
import { useEntityRegistry } from '../useEntityRegistry';
import DomainSearchResultItem from './DomainSearchResultItem';

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

const LoadingWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    height: 350px;
    font-size: 30px;
`;

function DomainSearch() {
    const [query, setQuery] = useState('');
    const [isSearchBarFocused, setIsSearchBarFocused] = useState(false);
    const entityRegistry = useEntityRegistry();
    const { data, loading } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: [EntityType.Domain],
                query,
                start: 0,
                count: 50,
            },
        },
    });

    const searchResults = data?.searchAcrossEntities?.searchResults;
    const timerRef = useRef(-1);

    const handleQueryChange = (q: string) => {
        window.clearTimeout(timerRef.current);
        timerRef.current = window.setTimeout(() => {
            setQuery(q);
        }, 250);
    };

    const renderLoadingIndicator = () => (
        <LoadingWrapper>
            <LoadingOutlined />
        </LoadingWrapper>
    );

    const renderSearchResults = () => (
        <ResultsWrapper>
            {searchResults?.map((result) => (
                <DomainSearchResultItem
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
                {loading && renderLoadingIndicator()}
                {!loading && isSearchBarFocused && !!searchResults?.length && renderSearchResults()}
            </ClickOutside>
        </DomainSearchWrapper>
    );
}

export default DomainSearch;
