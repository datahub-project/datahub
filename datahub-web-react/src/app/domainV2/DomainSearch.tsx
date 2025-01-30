import React, { useRef, useState } from 'react';
import { LoadingOutlined, SearchOutlined } from '@ant-design/icons';
import styled from 'styled-components/macro';
import { useGetAutoCompleteResultsQuery } from '../../graphql/search.generated';
import { EntityType } from '../../types.generated';
import { SearchBar } from '../searchV2/SearchBar';
import ClickOutside from '../shared/ClickOutside';
import { useEntityRegistry } from '../useEntityRegistry';
import DomainSearchResultItem from './DomainSearchResultItem';
import { ANTD_GRAY, REDESIGN_COLORS } from '../entityV2/shared/constants';

const DomainSearchWrapper = styled.div`
    flex-shrink: 0;
    position: relative;
`;

const ResultsWrapper = styled.div`
    background-color: white;
    border-radius: 5px;
    box-shadow: 0 3px 6px -4px rgb(0 0 0 / 12%), 0 6px 16px 0 rgb(0 0 0 / 8%), 0 9px 28px 8px rgb(0 0 0 / 5%);
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
    const [query, setQuery] = useState('');
    const [isSearchBarFocused, setIsSearchBarFocused] = useState(false);
    const entityRegistry = useEntityRegistry();
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
    const timerRef = useRef(-1);

    const handleQueryChange = (q: string) => {
        window.clearTimeout(timerRef.current);
        timerRef.current = window.setTimeout(() => {
            setQuery(q);
        }, 250);
    };

    return (
        <DomainSearchWrapper>
            {isCollapsed && unhideSidebar ? (
                <SearchIcon onClick={unhideSidebar} />
            ) : (
                <ClickOutside onClickOutside={() => setIsSearchBarFocused(false)}>
                    <SearchBar
                        initialQuery={query || ''}
                        placeholderText="Search Domains"
                        suggestions={[]}
                        hideRecommendations
                        style={{ padding: 9 }}
                        inputStyle={{
                            height: 30,
                            fontSize: 10,
                            fontWeight: 500,
                            backgroundColor: ANTD_GRAY[3],
                        }}
                        textColor={ANTD_GRAY[10]}
                        placeholderColor={REDESIGN_COLORS.PLACEHOLDER_PURPLE}
                        onSearch={() => null}
                        onQueryChange={(q) => handleQueryChange(q)}
                        entityRegistry={entityRegistry}
                        onFocus={() => setIsSearchBarFocused(true)}
                    />
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
