import React, { useState, useEffect, useMemo } from 'react';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { SearchBar, PageTitle, Pagination } from '@components';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { EntityType } from '@src/types.generated';
import { Message } from '@src/app/shared/Message';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import styled from 'styled-components';
import { PageContainer } from '../govern/structuredProperties/styledComponents';
import EmptyTags from './EmptyTags';
import TagsTable from './TagsTable';

const HeaderContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 0px;
`;

const SearchContainer = styled.div`
    display: flex;
    align-items: center;
    margin-bottom: 0px;
`;

// Simple loading indicator at the top of the page
const LoadingBar = styled.div`
    position: fixed;
    top: 0;
    left: 0;
    width: 100%;
    height: 4px;
    background-color: #1890ff;
    z-index: 1000;
    animation: loading 2s infinite ease-in-out;

    @keyframes loading {
        0% {
            opacity: 0.6;
        }
        50% {
            opacity: 1;
        }
        100% {
            opacity: 0.6;
        }
    }
`;

const PAGE_SIZE = 10;

const ManageTags = () => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const [searchQuery, setSearchQuery] = useState('');
    const [currentPage, setCurrentPage] = useState(1);
    const [debouncedSearchQuery, setDebouncedSearchQuery] = useState('*');
    const entityRegistry = useEntityRegistry();

    // Debounce search query input to reduce unnecessary renders
    useEffect(() => {
        const timer = setTimeout(() => {
            setDebouncedSearchQuery(searchQuery);
        }, 300); // 300ms delay

        return () => clearTimeout(timer);
    }, [searchQuery]);

    // Search query configuration
    const searchInputs = useMemo(
        () => ({
            types: [EntityType.Tag],
            query: debouncedSearchQuery,
            start: (currentPage - 1) * PAGE_SIZE,
            count: PAGE_SIZE,
            filters: [],
        }),
        [currentPage, debouncedSearchQuery],
    );

    const {
        data: searchData,
        loading: searchLoading,
        error: searchError,
        refetch,
        networkStatus,
    } = useGetSearchResultsForMultipleQuery({
        variables: { input: searchInputs },
        fetchPolicy: 'cache-first', // Changed from cache-and-network to prevent double loading
        notifyOnNetworkStatusChange: false, // Changed to false to reduce unnecessary re-renders
    });

    const totalTags = searchData?.searchAcrossEntities?.total || 0;

    // Check if we have results to display
    const hasSearchResults = useMemo(() => {
        const results = searchData?.searchAcrossEntities?.searchResults || [];
        if (debouncedSearchQuery) {
            // If there's a search query, check if any tags match
            return results.some((result) => {
                const tag = result.entity;
                const displayName = entityRegistry.getDisplayName(EntityType.Tag, tag);
                return displayName.toLowerCase().includes(debouncedSearchQuery.toLowerCase());
            });
        }
        // Otherwise just check if we have any results
        return results.length > 0;
    }, [searchData, debouncedSearchQuery, entityRegistry]);

    if (searchError) {
        return <Message type="error" content={`Failed to load tags: ${searchError.message}`} />;
    }

    return (
        <PageContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
            {searchLoading && <LoadingBar />}

            <HeaderContainer>
                <PageTitle title="Manage Tags" subTitle="Create and edit asset & column tags" />
            </HeaderContainer>

            <SearchContainer>
                <SearchBar
                    placeholder="Search tags..."
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e)}
                    data-testid="tag-search-input"
                />
            </SearchContainer>

            {!searchLoading && !hasSearchResults ? (
                <EmptyTags isEmptySearch={debouncedSearchQuery.length > 0} />
            ) : (
                <>
                    <TagsTable
                        searchQuery={debouncedSearchQuery}
                        searchData={searchData}
                        loading={searchLoading}
                        networkStatus={networkStatus}
                        refetch={refetch}
                    />
                    <Pagination
                        currentPage={currentPage}
                        itemsPerPage={PAGE_SIZE}
                        totalPages={totalTags}
                        loading={searchLoading}
                        onPageChange={(page) => setCurrentPage(page)}
                    />
                </>
            )}
        </PageContainer>
    );
};

export { ManageTags };
