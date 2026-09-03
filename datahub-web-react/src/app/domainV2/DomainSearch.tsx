import { Loader, SearchBar } from '@components';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useDebounce } from 'react-use';
import styled from 'styled-components/macro';

import DomainSearchResultItem from '@app/domainV2/DomainSearchResultItem';
import ClickOutside from '@app/shared/ClickOutside';
import {
    SearchIconButton,
    SearchResultsDropdown,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar.components';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetAutoCompleteResultsQuery } from '@graphql/search.generated';
import { EntityType } from '@types';

const DomainSearchWrapper = styled.div`
    position: relative;
`;

const LoadingWrapper = styled(SearchResultsDropdown)`
    display: flex;
    justify-content: center;
    padding: 16px 0;
    font-size: 16px;
`;

type Props = {
    isCollapsed?: boolean;
    unhideSidebar?: () => void;
};

function DomainSearch({ isCollapsed, unhideSidebar }: Props) {
    const { t: tc } = useTranslation('common.actions');
    const [searchInput, setSearchInput] = useState('');
    const [query, setQuery] = useState('');
    const [isSearchBarFocused, setIsSearchBarFocused] = useState(false);
    const entityRegistry = useEntityRegistry();

    useDebounce(() => setQuery(searchInput), 200, [searchInput]);
    const { data, loading } = useGetAutoCompleteResultsQuery({
        variables: {
            input: {
                type: EntityType.Domain,
                query,
            },
        },
        skip: !query,
    });

    const entities = data?.autoComplete?.entities || [];

    return (
        <DomainSearchWrapper>
            {isCollapsed && unhideSidebar ? (
                <SearchIconButton
                    type="button"
                    onClick={unhideSidebar}
                    aria-label={tc('search')}
                    data-testid="domain-sidebar-search-icon"
                >
                    <MagnifyingGlass size={20} weight="regular" />
                </SearchIconButton>
            ) : (
                <ClickOutside onClickOutside={() => setIsSearchBarFocused(false)}>
                    <SearchBar
                        placeholder={tc('search')}
                        value={searchInput}
                        onChange={setSearchInput}
                        onFocus={() => setIsSearchBarFocused(true)}
                    />
                    {loading && isSearchBarFocused && (
                        <LoadingWrapper>
                            <Loader size="md" />
                        </LoadingWrapper>
                    )}
                    {!loading && isSearchBarFocused && !!entities?.length && (
                        <SearchResultsDropdown data-testid="search-results">
                            {entities?.map((entity) => (
                                <DomainSearchResultItem
                                    key={entity.urn}
                                    entity={entity}
                                    entityRegistry={entityRegistry}
                                    query={query}
                                    onResultClick={() => setIsSearchBarFocused(false)}
                                />
                            ))}
                        </SearchResultsDropdown>
                    )}
                </ClickOutside>
            )}
        </DomainSearchWrapper>
    );
}

export default DomainSearch;
