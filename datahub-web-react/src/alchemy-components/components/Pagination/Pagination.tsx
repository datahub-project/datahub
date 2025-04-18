import React from 'react';
import { Pagination as PaginationComponent } from 'antd';
import { paginationDefaults, PaginationProps } from './types';
import { PaginationContainer } from './components';

export const Pagination = ({
    currentPage = paginationDefaults.currentPage,
    itemsPerPage = paginationDefaults.itemsPerPage,
    totalPages = paginationDefaults.totalPages,
    loading = paginationDefaults.loading,
    onPageChange,
    ...props
}: PaginationProps) => {
    if (loading) {
        return null;
    }
    return (
        <PaginationContainer>
            <PaginationComponent
                {...props}
                current={currentPage}
                pageSize={itemsPerPage}
                total={totalPages}
                onChange={onPageChange}
            />
        </PaginationContainer>
    );
};
