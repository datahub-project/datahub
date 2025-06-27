import { Button, PageTitle, Pagination, SearchBar, StructuredPopover } from '@components';
import React, { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { PageContainer } from '@app/govern/structuredProperties/styledComponents';
import CreateNewTagModal from '@app/tags/CreateNewTagModal/CreateNewTagModal';
import EmptyTags from '@app/tags/EmptyTags';
import TagsTable from '@app/tags/TagsTable';
import { Message } from '@src/app/shared/Message';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { EntityType } from '@src/types.generated';

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
    const [pageSize, setPageSize] = useState(PAGE_SIZE);
    const [debouncedSearchQuery, setDebouncedSearchQuery] = useState('*');
    const entityRegistry = useEntityRegistry();
    const [showCreateTagModal, setShowCreateTagModal] = useState(false);

    // Check permissions using UserContext
    const userContext = useUserContext();
    const canCreateTags = userContext?.platformPrivileges?.createTags || userContext?.platformPrivileges?.manageTags;

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
            start: (currentPage - 1) * pageSize,
            count: pageSize,
            filters: [],
        }),
        [currentPage, debouncedSearchQuery, pageSize],
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

    // Create the Create Tag button with proper permissions handling
    const renderCreateTagButton = () => {
        if (!canCreateTags) {
            return (
                <StructuredPopover
                    title="You do not have permission to create tags"
                    placement="left"
                    showArrow
                    mouseEnterDelay={0.1}
                    mouseLeaveDelay={0.1}
                >
                    <span>
                        <Button size="md" color="violet" icon={{ icon: 'Plus', source: 'phosphor' }} disabled>
                            Create Tag
                        </Button>
                    </span>
                </StructuredPopover>
            );
        }

        return (
            <Button
                onClick={() => setShowCreateTagModal(true)}
                size="md"
                color="violet"
                icon={{ icon: 'Plus', source: 'phosphor' }}
            >
                Create Tag
            </Button>
        );
    };

    return (
        <PageContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
            {searchLoading && <LoadingBar />}

            <HeaderContainer>
                <PageTitle title="Manage Tags" subTitle="Create and edit asset & column tags" />
                {renderCreateTagButton()}
            </HeaderContainer>

            <SearchContainer>
                <SearchBar
                    placeholder="Search tags..."
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e)}
                    data-testid="tag-search-input"
                    width="280px"
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
                        itemsPerPage={pageSize}
                        totalPages={totalTags}
                        loading={searchLoading}
                        onPageChange={(newPage, newPageSize) => {
                            if (newPageSize !== pageSize) {
                                setCurrentPage(1);
                                setPageSize(newPageSize);
                            } else {
                                setCurrentPage(newPage);
                            }
                        }}
                    />
                </>
            )}

            <CreateNewTagModal
                open={showCreateTagModal}
                onClose={() => {
                    setShowCreateTagModal(false);
                    setTimeout(() => refetch(), 3000);
                }}
            />
        </PageContainer>
    );
};

export { ManageTags };
