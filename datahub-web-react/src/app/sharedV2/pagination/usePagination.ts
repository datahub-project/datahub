import { useState } from 'react';

export interface Pagination {
    page: number;
    setPage: React.Dispatch<React.SetStateAction<number>>;
    pageSize: number;
    setPageSize: React.Dispatch<React.SetStateAction<number>>;
    start: number;
    count: number;
}

export default function usePagination(defaultPageSize?: number) {
    const [page, setPage] = useState(1);
    const [pageSize, setPageSize] = useState(defaultPageSize || 10);

    const start = (page - 1) * pageSize;

    return { page, setPage, pageSize, setPageSize, start, count: pageSize } as Pagination;
}
