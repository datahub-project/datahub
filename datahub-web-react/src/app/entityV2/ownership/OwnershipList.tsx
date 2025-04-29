import { message } from 'antd';
import React, { useCallback, useState } from 'react';
import styled from 'styled-components/macro';

import { OwnershipTable } from '@app/entityV2/ownership/table/OwnershipTable';
import TabToolbar from '@app/entityV2/shared/components/styled/TabToolbar';
import { Message } from '@app/shared/Message';
import { SearchBar } from '@src/alchemy-components';

import { useListOwnershipTypesQuery } from '@graphql/ownership.generated';

const ListContainer = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    min-height: 0;
`;

const PAGE_SIZE = 20;

/**
 * This component renders a scrollable list of Ownership Types.
 */
export const OwnershipList = () => {
    const [query, setQuery] = useState<undefined | string>(undefined);
    const [page, setPage] = useState(0);
    const [ownershipTypesList, setOwnershipTypesList] = useState<any[]>([]);

    const { data, loading, error, refetch, fetchMore } = useListOwnershipTypesQuery({
        variables: {
            input: {
                start: 0,
                count: PAGE_SIZE,
                query,
            },
        },
        onCompleted: (result) => {
            const filteredTypes =
                result?.listOwnershipTypes?.ownershipTypes?.filter(
                    (type) => type.urn !== 'urn:li:ownershipType:none',
                ) || [];
            setOwnershipTypesList(filteredTypes);
            setPage(1);
        },
    });

    const hasMore = (data?.listOwnershipTypes?.total || 0) > ownershipTypesList.length;

    const handleLoadMore = useCallback(() => {
        if (!hasMore || loading) return;

        fetchMore({
            variables: {
                input: {
                    start: page * PAGE_SIZE,
                    count: PAGE_SIZE,
                    query,
                },
            },
        }).then((fetchMoreResult) => {
            const newOwnershipTypes =
                fetchMoreResult.data?.listOwnershipTypes?.ownershipTypes?.filter(
                    (type) => type.urn !== 'urn:li:ownershipType:none',
                ) || [];

            setOwnershipTypesList([...ownershipTypesList, ...newOwnershipTypes]);
            setPage(page + 1);
        });
    }, [fetchMore, hasMore, loading, ownershipTypesList, page, query]);

    const handleSearch = (value: string) => {
        setQuery(value.length > 0 ? value : undefined);
        // Reset pagination when search changes
        setPage(0);
        setOwnershipTypesList([]);
    };

    return (
        <ListContainer>
            {!data && loading && <Message type="loading" content="Loading Ownership Types..." />}
            {error &&
                message.error({
                    content: `Failed to load Ownership Types! An unexpected error occurred.`,
                    duration: 3,
                })}
            <TabToolbar>
                <SearchBar placeholder="Search..." value={query || ''} onChange={handleSearch} width="280px" />
            </TabToolbar>
            <OwnershipTable
                ownershipTypes={ownershipTypesList}
                setIsOpen={() => {}}
                setOwnershipType={() => {}}
                refetch={refetch}
                hasMore={hasMore}
                isLoadingMore={loading && page > 0}
                onLoadMore={handleLoadMore}
            />
        </ListContainer>
    );
};
