import { PaginationProps as AntPaginationProps } from 'antd';

export interface PaginationProps extends AntPaginationProps {
    currentPage: number;
    itemsPerPage: number;
    totalPages: number;
    onPageChange?: (page: number, pageSize: number) => void;
    loading?: boolean;
}

export const paginationDefaults: PaginationProps = {
    currentPage: 1,
    itemsPerPage: 1,
    totalPages: 10,
    loading: false,
};
