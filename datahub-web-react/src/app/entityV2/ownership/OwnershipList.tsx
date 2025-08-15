import { PlusOutlined } from '@ant-design/icons';
import { Button, Pagination, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';

import { OwnershipBuilderModal } from '@app/entityV2/ownership/OwnershipBuilderModal';
import { OwnershipTable } from '@app/entityV2/ownership/table/OwnershipTable';
import TabToolbar from '@app/entityV2/shared/components/styled/TabToolbar';
import { SearchBar } from '@app/search/SearchBar';
import { Message } from '@app/shared/Message';
import { scrollToTop } from '@app/shared/searchUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useListOwnershipTypesQuery } from '@graphql/ownership.generated';
import { OwnershipTypeEntity } from '@types';

const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const StyledPagination = styled(Pagination)`
    margin: 40px;
`;

const searchBarStyle = {
    maxWidth: 220,
    padding: 0,
};

const searchBarInputStyle = {
    height: 32,
    fontSize: 12,
};

/**
 * This component renders a paginated, searchable list of Ownership Types.
 */
export const OwnershipList = () => {
    /**
     * Context
     */
    const entityRegistry = useEntityRegistry();

    /**
     * State
     */
    const [page, setPage] = useState(1);
    const [showOwnershipBuilder, setShowOwnershipBuilder] = useState<boolean>(false);
    const [ownershipType, setOwnershipType] = useState<undefined | OwnershipTypeEntity>(undefined);
    const [query, setQuery] = useState<undefined | string>(undefined);

    /**
     * Queries
     */
    const pageSize = 10;
    const start: number = (page - 1) * pageSize;
    const { data, loading, error, refetch } = useListOwnershipTypesQuery({
        variables: {
            input: {
                start,
                count: pageSize,
                query,
            },
        },
    });
    const totalOwnershipTypes = data?.listOwnershipTypes?.total || 0;
    const ownershipTypes =
        data?.listOwnershipTypes?.ownershipTypes?.filter((type) => type.urn !== 'urn:li:ownershipType:none') || [];

    const onClickCreateOwnershipType = () => {
        setShowOwnershipBuilder(true);
    };

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
            {error &&
                message.error({
                    content: `Failed to load Ownership Types! An unexpected error occurred.`,
                    duration: 3,
                })}
            <TabToolbar>
                <Button type="text" onClick={onClickCreateOwnershipType} data-testid="create-owner-type-v2">
                    <PlusOutlined /> Create Ownership Type
                </Button>
                <SearchBar
                    initialQuery=""
                    placeholderText="Search by Name..."
                    suggestions={[]}
                    style={searchBarStyle}
                    inputStyle={searchBarInputStyle}
                    onSearch={() => null}
                    onQueryChange={(q) => setQuery(q.length > 0 ? q : undefined)}
                    entityRegistry={entityRegistry}
                />
            </TabToolbar>
            <OwnershipTable
                ownershipTypes={ownershipTypes}
                setIsOpen={setShowOwnershipBuilder}
                setOwnershipType={setOwnershipType}
                refetch={refetch}
            />
            {totalOwnershipTypes >= pageSize && (
                <PaginationContainer>
                    <StyledPagination
                        current={page}
                        pageSize={pageSize}
                        total={totalOwnershipTypes}
                        showLessItems
                        onChange={onChangePage}
                        showSizeChanger={false}
                    />
                </PaginationContainer>
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
