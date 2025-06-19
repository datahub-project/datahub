import { Button, PageTitle, Pagination, SearchBar, StructuredPopover } from '@components';
import React, { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import ApplicationsTable from '@app/applications/ApplicationsTable';
import CreateNewApplicationModal from '@app/applications/CreateNewApplicationModal/CreateNewApplicationModal';
import EmptyApplications from '@app/applications/EmptyApplications';
import { useUserContext } from '@app/context/useUserContext';
import { PageContainer } from '@app/govern/structuredProperties/styledComponents';
import { Message } from '@src/app/shared/Message';
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

const ManageApplications = () => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const [searchQuery, setSearchQuery] = useState('');
    const [currentPage, setCurrentPage] = useState(1);
    const [debouncedSearchQuery, setDebouncedSearchQuery] = useState('*');
    const [showCreateApplicationModal, setShowCreateApplicationModal] = useState(false);

    const userContext = useUserContext();
    const canManageApplications = userContext?.platformPrivileges?.manageApplications;

    // Debounce search query input to reduce unnecessary renders
    useEffect(() => {
        const timer = setTimeout(() => {
            setDebouncedSearchQuery(searchQuery);
        }, 300);

        return () => clearTimeout(timer);
    }, [searchQuery]);

    // Search query configuration
    const searchInputs = useMemo(
        () => ({
            types: [EntityType.Application],
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
        fetchPolicy: 'cache-first',
    });

    const totalApplications = searchData?.searchAcrossEntities?.total || 0;

    if (searchError) {
        return <Message type="error" content={`Failed to load applications: ${searchError.message}`} />;
    }
    // Create the Create Application button with proper permissions handling
    const renderCreateApplicationButton = () => {
        if (!canManageApplications) {
            return (
                <StructuredPopover
                    title="You do not have permission to create applications"
                    placement="left"
                    showArrow
                    mouseEnterDelay={0.1}
                    mouseLeaveDelay={0.1}
                >
                    <span>
                        <Button size="md" color="violet" icon={{ icon: 'Plus', source: 'phosphor' }} disabled>
                            Create Application
                        </Button>
                    </span>
                </StructuredPopover>
            );
        }

        return (
            <Button
                onClick={() => setShowCreateApplicationModal(true)}
                size="md"
                color="violet"
                icon={{ icon: 'Plus', source: 'phosphor' }}
            >
                Create Application
            </Button>
        );
    };

    return (
        <PageContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
            {searchLoading && <LoadingBar />}

            <HeaderContainer>
                <PageTitle title="Manage Applications" subTitle="Create and edit applications" />
                {renderCreateApplicationButton()}
            </HeaderContainer>

            <SearchContainer>
                <SearchBar
                    placeholder="Search applications..."
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e)}
                    id="application-search-input"
                    data-testid="application-search-input"
                    width="280px"
                />
            </SearchContainer>

            {!searchLoading && totalApplications === 0 ? (
                <EmptyApplications isEmptySearch={debouncedSearchQuery.length > 0} />
            ) : (
                <>
                    <ApplicationsTable
                        searchQuery={debouncedSearchQuery}
                        searchData={searchData}
                        loading={searchLoading}
                        networkStatus={networkStatus}
                        refetch={refetch}
                    />
                    <Pagination
                        currentPage={currentPage}
                        itemsPerPage={PAGE_SIZE}
                        totalPages={totalApplications}
                        loading={searchLoading}
                        onPageChange={(page) => setCurrentPage(page)}
                    />
                </>
            )}

            <CreateNewApplicationModal
                open={showCreateApplicationModal}
                onClose={() => {
                    setShowCreateApplicationModal(false);
                    setTimeout(() => refetch(), 3000);
                }}
            />
        </PageContainer>
    );
};

export { ManageApplications };
