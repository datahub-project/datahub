import { useState } from 'react';

export interface Pagination {
    page: number;
    setPage: (newPage: number) => void;
    pageSize: number;
    setPageSize: (newPageSize: number) => void;
    start: number;
    count: number;
}

export default function usePagination(defaultPageSize?: number, initialPage?: number) {
    const [page, setPage] = useState(initialPage ?? 1);
    const [pageSize, setPageSize] = useState(defaultPageSize || 10);

    const start = (page - 1) * pageSize;

    return { page, setPage, pageSize, setPageSize, start, count: pageSize } as Pagination;
}
