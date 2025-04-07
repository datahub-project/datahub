import React, { useState, useEffect, useMemo } from 'react';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { SearchBar, PageTitle, Pagination, Button, Tooltip2 } from '@components';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { EntityType } from '@src/types.generated';
import { Message } from '@src/app/shared/Message';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import styled from 'styled-components';
import { PageContainer } from '../govern/structuredProperties/styledComponents';
import EmptyTags from './EmptyTags';
import TagsTable from './TagsTable';
import CreateNewTagModal from './CreateNewTagModal';
import { useUserContext } from '../context/useUserContext';

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

    // Create the Create Tag button with proper permissions handling
    const renderCreateTagButton = () => {
        if (!canCreateTags) {
            // Option: don't show the button at all if no permissions
            // return null;

            // Option: show disabled button with tooltip
            return (
                <Tooltip2
                    title="Permission Required"
                    sections={[
                        {
                            title: 'Error',
                            content: 'You do not have permission to create tags',
                        },
                    ]}
                    placement="bottom"
                >
                    <Button size="md" color="violet" icon={{ icon: 'Plus', source: 'phosphor' }} disabled>
                        Create Tag
                    </Button>
                </Tooltip2>
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
                        itemsPerPage={PAGE_SIZE}
                        totalPages={totalTags}
                        loading={searchLoading}
                        onPageChange={(page) => setCurrentPage(page)}
                    />
                </>
            )}

            <CreateNewTagModal
                open={showCreateTagModal}
                onClose={() => {
                    setShowCreateTagModal(false);
                    refetch(); // Refresh the tag list after creating a new tag
                }}
            />
        </PageContainer>
    );
};

export { ManageTags };
