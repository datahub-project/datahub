import React, { useState } from 'react';
import { Button, Pagination, message } from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import styled from 'styled-components/macro';
import { useListOwnershipTypesQuery } from '../../../graphql/ownership.generated';
import { Message } from '../../shared/Message';
import { OwnershipBuilderModal } from './OwnershipBuilderModal';
import TabToolbar from '../shared/components/styled/TabToolbar';
import { OwnershipTable } from './table/OwnershipTable';
import { OwnershipTypeEntity } from '../../../types.generated';
import { SearchBar } from '../../search/SearchBar';
import { useEntityRegistry } from '../../useEntityRegistry';
import { scrollToTop } from '../../shared/searchUtils';

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
                <Button type="text" onClick={onClickCreateOwnershipType}>
                    <PlusOutlined /> Create new Ownership Type
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
