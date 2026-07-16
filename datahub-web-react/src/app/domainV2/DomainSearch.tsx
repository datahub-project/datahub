import { Loader, SearchBar } from '@components';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useDebounce } from 'react-use';
import styled from 'styled-components/macro';

import DomainSearchResultItem from '@app/domainV2/DomainSearchResultItem';
import ClickOutside from '@app/shared/ClickOutside';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetAutoCompleteResultsQuery } from '@graphql/search.generated';
import { EntityType } from '@types';

const DomainSearchWrapper = styled.div`
    flex-shrink: 0;
    position: relative;
`;

const ResultsWrapper = styled.div`
    background-color: ${(props) => props.theme.colors.bg};
    border-radius: 5px;
    box-shadow: ${(props) => props.theme.colors.shadowMd};
    padding: 8px;
    position: absolute;
    max-height: 210px;
    overflow: auto;
    width: calc(100% - 8px);
    left: 4px;
    top: 55px;
    z-index: 1;
`;

const LoadingWrapper = styled(ResultsWrapper)`
    display: flex;
    justify-content: center;
    padding: 16px 0;
    font-size: 16px;
`;

const InputWrapper = styled.div`
    padding: 12px;
`;

// Collapsed-state search trigger. Anatomy mirrors the documents sidebar's
// `SearchIconButton` so both sidebars present the same collapsed entry point:
// a full-width borderless button that re-expands the sidebar on click.
const SearchIconButton = styled.button`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 100%;
    padding: 16px 0;
    border: none;
    background: transparent;
    cursor: pointer;
    color: ${(props) => props.theme.colors.icon};

    &:hover {
        color: ${(props) => props.theme.colors.iconHover};
    }
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
                    <InputWrapper>
                        <SearchBar
                            placeholder={tc('search')}
                            value={searchInput}
                            onChange={setSearchInput}
                            onFocus={() => setIsSearchBarFocused(true)}
                        />
                    </InputWrapper>
                    {loading && isSearchBarFocused && (
                        <LoadingWrapper>
                            <Loader size="md" />
                        </LoadingWrapper>
                    )}
                    {!loading && isSearchBarFocused && !!entities?.length && (
                        <ResultsWrapper data-testid="search-results">
                            {entities?.map((entity) => (
                                <DomainSearchResultItem
                                    key={entity.urn}
                                    entity={entity}
                                    entityRegistry={entityRegistry}
                                    query={query}
                                    onResultClick={() => setIsSearchBarFocused(false)}
                                />
                            ))}
                        </ResultsWrapper>
                    )}
                </ClickOutside>
            )}
        </DomainSearchWrapper>
    );
}

export default DomainSearch;
