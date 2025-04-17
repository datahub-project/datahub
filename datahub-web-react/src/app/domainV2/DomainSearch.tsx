import { LoadingOutlined, SearchOutlined } from '@ant-design/icons';
import { SearchBar } from '@components';
import React, { useState } from 'react';
import { useDebounce } from 'react-use';
import styled from 'styled-components/macro';

import DomainSearchResultItem from '@app/domainV2/DomainSearchResultItem';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import ClickOutside from '@app/shared/ClickOutside';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetAutoCompleteResultsQuery } from '@graphql/search.generated';
import { EntityType } from '@types';

const DomainSearchWrapper = styled.div`
    flex-shrink: 0;
    position: relative;
`;

const ResultsWrapper = styled.div`
    background-color: white;
    border-radius: 5px;
    box-shadow:
        0 3px 6px -4px rgb(0 0 0 / 12%),
        0 6px 16px 0 rgb(0 0 0 / 8%),
        0 9px 28px 8px rgb(0 0 0 / 5%);
    padding: 8px;
    position: absolute;
    max-height: 210px;
    overflow: auto;
    width: calc(100% - 32px);
    left: 16px;
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

const SearchIcon = styled(SearchOutlined)`
    color: ${REDESIGN_COLORS.TEXT_HEADING_SUB_LINK};
    padding: 16px;
    width: 100%;
    font-size: 20px;
`;

type Props = {
    isCollapsed?: boolean;
    unhideSidebar?: () => void;
};

function DomainSearch({ isCollapsed, unhideSidebar }: Props) {
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
                <SearchIcon onClick={unhideSidebar} />
            ) : (
                <ClickOutside onClickOutside={() => setIsSearchBarFocused(false)}>
                    <InputWrapper>
                        <SearchBar
                            placeholder="Search"
                            value={searchInput}
                            onChange={setSearchInput}
                            onFocus={() => setIsSearchBarFocused(true)}
                        />
                    </InputWrapper>
                    {loading && isSearchBarFocused && (
                        <LoadingWrapper>
                            <LoadingOutlined />
                        </LoadingWrapper>
                    )}
                    {!loading && isSearchBarFocused && !!entities?.length && (
                        <ResultsWrapper>
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
