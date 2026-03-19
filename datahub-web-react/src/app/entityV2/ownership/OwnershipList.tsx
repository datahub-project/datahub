import { EmptyState, Pagination, SearchBar, toast } from '@components';
import { Users } from '@phosphor-icons/react/dist/csr/Users';
import React, { useState } from 'react';

import { OwnershipBuilderModal } from '@app/entityV2/ownership/OwnershipBuilderModal';
import { OwnershipTable } from '@app/entityV2/ownership/table/OwnershipTable';
import { Message } from '@app/shared/Message';
import { scrollToTop } from '@app/shared/searchUtils';

import { useListOwnershipTypesQuery } from '@graphql/ownership.generated';
import { OwnershipTypeEntity } from '@types';

type OwnershipListProps = {
    showOwnershipBuilder: boolean;
    setShowOwnershipBuilder: (show: boolean) => void;
};

/**
 * This component renders a paginated, searchable list of Ownership Types.
 */
export const OwnershipList = ({ showOwnershipBuilder, setShowOwnershipBuilder }: OwnershipListProps) => {
    const [page, setPage] = useState(1);
    const [ownershipType, setOwnershipType] = useState<undefined | OwnershipTypeEntity>(undefined);
    const [query, setQuery] = useState('');

    const pageSize = 10;
    const start: number = (page - 1) * pageSize;
    const { data, loading, error, refetch } = useListOwnershipTypesQuery({
        variables: {
            input: {
                start,
                count: pageSize,
                query: query.length > 0 ? query : undefined,
            },
        },
    });
    const totalOwnershipTypes = data?.listOwnershipTypes?.total || 0;
    const ownershipTypes =
        data?.listOwnershipTypes?.ownershipTypes?.filter((type) => type.urn !== 'urn:li:ownershipType:none') || [];

    const onCloseModal = () => {
        setShowOwnershipBuilder(false);
        setOwnershipType(undefined);
    };

    const onChangePage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    return (
        <>
            {!data && loading && <Message type="loading" content="Loading Ownership Types..." />}
            {error && toast.error('Failed to load Ownership Types! An unexpected error occurred.')}
            <SearchBar
                placeholder="Search..."
                value={query}
                onChange={(value) => {
                    setQuery(value);
                    setPage(1);
                }}
                width="300px"
                allowClear
            />
            {ownershipTypes.length > 0 ? (
                <>
                    <OwnershipTable
                        ownershipTypes={ownershipTypes}
                        setIsOpen={setShowOwnershipBuilder}
                        setOwnershipType={setOwnershipType}
                        refetch={refetch}
                    />
                    {totalOwnershipTypes >= pageSize && (
                        <Pagination
                            currentPage={page}
                            itemsPerPage={pageSize}
                            total={totalOwnershipTypes}
                            showLessItems
                            onPageChange={onChangePage}
                            showSizeChanger={false}
                        />
                    )}
                </>
            ) : (
                !loading && (
                    <EmptyState
                        title="No Ownership Types found"
                        description="Create a custom ownership type to categorize asset owners."
                        icon={Users}
                        action={{
                            label: 'Create Ownership Type',
                            onClick: () => setShowOwnershipBuilder(true),
                        }}
                        style={{ flex: 1, justifyContent: 'center' }}
                    />
                )
            )}
            <OwnershipBuilderModal
                isOpen={showOwnershipBuilder}
                onClose={onCloseModal}
                refetch={refetch}
                ownershipType={ownershipType}
            />
        </>
    );
};
