import { Pagination as PaginationComponent } from 'antd';
import React from 'react';

import { PaginationContainer } from '@components/components/Pagination/components';
import { PaginationProps, paginationDefaults } from '@components/components/Pagination/types';

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
