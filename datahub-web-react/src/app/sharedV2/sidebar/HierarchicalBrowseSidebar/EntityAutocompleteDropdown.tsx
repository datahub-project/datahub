import { Loader, SearchBar } from '@components';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { useDebounce } from 'react-use';
import styled from 'styled-components';

import { IconStyleType } from '@app/entityV2/Entity';
import ClickOutside from '@app/shared/ClickOutside';
import { SearchResultsDropdown } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar.components';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetAutoCompleteMultipleResultsQuery } from '@graphql/search.generated';
import { EntityType } from '@types';

const SearchWrapper = styled.div`
    position: relative;
`;

const LoadingWrapper = styled.div`
    display: flex;
    justify-content: center;
    padding: 4px 0;
    font-size: 16px;
`;

const SearchResult = styled(Link)`
    color: ${(props) => props.theme.colors.text};
    display: inline-block;
    height: 100%;
    padding: 6px 8px;
    width: 100%;

    &:hover {
        background-color: ${(props) => props.theme.colors.bgSurface};
        color: ${(props) => props.theme.colors.text};
    }
`;

const IconWrapper = styled.span`
    margin-right: 8px;
`;

type Props = {
    entityTypes: EntityType[];
    placeholder: string;
    dataTestId?: string;
};

/**
 * Debounced autocomplete search bar with dropdown results, shared by hierarchical
 * browse sidebars (glossary, marketplace, etc.).
 */
export default function EntityAutocompleteDropdown({ entityTypes, placeholder, dataTestId }: Props) {
    const [searchInput, setSearchInput] = useState('');
    const [query, setQuery] = useState('');
    const [isSearchBarFocused, setIsSearchBarFocused] = useState(false);
    const entityRegistry = useEntityRegistry();

    useDebounce(() => setQuery(searchInput), 200, [searchInput]);

    const { data, loading } = useGetAutoCompleteMultipleResultsQuery({
        variables: {
            input: {
                types: entityTypes,
                query,
                limit: 50,
            },
        },
        skip: !query,
    });

    const searchResults = data?.autoCompleteForMultiple?.suggestions?.flatMap((suggestion) => suggestion.entities);

    return (
        <SearchWrapper>
            <ClickOutside onClickOutside={() => setIsSearchBarFocused(false)}>
                <SearchBar
                    placeholder={placeholder}
                    value={searchInput}
                    onChange={setSearchInput}
                    onFocus={() => setIsSearchBarFocused(true)}
                    data-testid={dataTestId}
                />
                {isSearchBarFocused && searchInput === query && (loading || !!searchResults?.length) && (
                    <SearchResultsDropdown>
                        {loading && (
                            <LoadingWrapper>
                                <Loader size="md" />
                            </LoadingWrapper>
                        )}
                        {!loading &&
                            searchResults?.map((result) => (
                                <SearchResult
                                    key={result.urn}
                                    to={`${entityRegistry.getEntityUrl(result.type, result.urn)}`}
                                    onClick={() => setIsSearchBarFocused(false)}
                                >
                                    <IconWrapper>
                                        {entityRegistry.getIcon(result.type, 12, IconStyleType.ACCENT)}
                                    </IconWrapper>
                                    {entityRegistry.getDisplayName(result.type, result)}
                                </SearchResult>
                            ))}
                    </SearchResultsDropdown>
                )}
            </ClickOutside>
        </SearchWrapper>
    );
}
