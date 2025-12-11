/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { PaginationProps as AntPaginationProps } from 'antd';

export interface PaginationProps extends AntPaginationProps {
    currentPage: number;
    itemsPerPage: number;
    total: number;
    onPageChange?: (page: number, pageSize: number) => void;
    loading?: boolean;
}

export const paginationDefaults: PaginationProps = {
    currentPage: 1,
    itemsPerPage: 1,
    total: 10,
    loading: false,
};
