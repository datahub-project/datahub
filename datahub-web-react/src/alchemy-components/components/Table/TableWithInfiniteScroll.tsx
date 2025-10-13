import { Loader, Table } from '@components';
import React, { useEffect } from 'react';
import styled from 'styled-components';

import { useInfiniteScroll } from '@components/components/InfiniteScrollList/useInfiniteScroll';

const ScrollObserver = styled.div`
    width: 100%;
    height: 1px;
`;

const LoaderContainer = styled.td`
    padding: 10px;
`;

interface TableWithInfiniteScrollProps<T> {
    columns: any[];
    fetchData: (start: number, count: number) => Promise<T[]>;
    pageSize?: number;
    totalItemCount?: number;
    newItemToAdd?: T;
    itemToRemove?: (item: T) => boolean;
    itemToUpdate?: { updatedItem: T; shouldUpdate: (item: T) => boolean };
    triggerReset?: string | number | boolean;
}

export function TableWithInfiniteScroll<T>({
    columns,
    fetchData,
    pageSize = 10,
    totalItemCount = 0,
    newItemToAdd,
    itemToRemove,
    itemToUpdate,
    triggerReset,
}: TableWithInfiniteScrollProps<T>) {
    const {
        items: data,
        loading,
        hasMore,
        observerRef,
        prependItem,
        removeItem,
        updateItem,
    } = useInfiniteScroll<T>({
        fetchData,
        pageSize,
        totalItemCount,
        triggerReset,
    });

    // Update states to show immediate feedback on the UI

    useEffect(() => {
        if (newItemToAdd) {
            prependItem(newItemToAdd);
        }
    }, [newItemToAdd, prependItem]);

    useEffect(() => {
        if (itemToRemove) {
            removeItem(itemToRemove);
        }
    }, [itemToRemove, removeItem]);

    useEffect(() => {
        if (itemToUpdate) {
            updateItem(itemToUpdate.updatedItem, itemToUpdate.shouldUpdate);
        }
    }, [itemToUpdate, updateItem]);

    return (
        <Table
            columns={columns}
            data={data}
            isLoading={loading && data.length === 0}
            isScrollable
            renderScrollObserver={() => {
                return (
                    <>
                        {hasMore && (
                            <tr>
                                <td colSpan={columns.length}>
                                    <ScrollObserver ref={observerRef} />
                                </td>
                            </tr>
                        )}
                        {data.length > 0 && loading && (
                            <tr>
                                <LoaderContainer colSpan={columns.length}>
                                    <Loader size="sm" alignItems="center" />
                                </LoaderContainer>
                            </tr>
                        )}
                    </>
                );
            }}
        />
    );
}
