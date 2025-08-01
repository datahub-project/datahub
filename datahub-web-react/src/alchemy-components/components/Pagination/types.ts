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
