/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Pagination as PaginationComponent } from 'antd';
import React from 'react';

import { PaginationContainer } from '@components/components/Pagination/components';
import { PaginationProps, paginationDefaults } from '@components/components/Pagination/types';

export const Pagination = ({
    currentPage = paginationDefaults.currentPage,
    itemsPerPage = paginationDefaults.itemsPerPage,
    total = paginationDefaults.total,
    loading = paginationDefaults.loading,
    onPageChange,
    className,
    ...props
}: PaginationProps) => {
    if (loading) {
        return null;
    }
    return (
        <PaginationContainer className={className}>
            <PaginationComponent
                {...props}
                current={currentPage}
                pageSize={itemsPerPage}
                total={total}
                onChange={onPageChange}
            />
        </PaginationContainer>
    );
};
