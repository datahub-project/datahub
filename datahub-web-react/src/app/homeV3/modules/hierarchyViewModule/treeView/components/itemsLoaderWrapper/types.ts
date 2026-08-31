export interface NodesLoaderWrapperProps {
    total: number;
    // how many items already loaded
    current: number;
    pageSize: number;
    depth: number;
    enabled?: boolean;
    loading?: boolean;
    onLoad: () => void;
}
