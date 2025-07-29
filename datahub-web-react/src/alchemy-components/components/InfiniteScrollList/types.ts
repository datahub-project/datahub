export interface InfiniteScrollListProps<T> {
    fetchData: (start: number, count: number) => Promise<T[]>;
    renderItem: (item: T) => React.ReactNode;
    pageSize?: number;
    emptyState?: React.ReactNode;
    totalItemCount?: number;
}
