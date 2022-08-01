import React, { useState } from 'react';
import { Button, Modal } from 'antd';
import { FacetFilterInput } from '../../../../../../types.generated';
import { EmbeddedListSearch } from './EmbeddedListSearch';

type Props = {
    emptySearchQuery?: string | null;
    fixedFilter?: FacetFilterInput | null;
    fixedQuery?: string | null;
    placeholderText?: string | null;
    defaultShowFilters?: boolean;
    defaultFilters?: Array<FacetFilterInput>;
    onClose?: () => void;
    searchBarStyle?: any;
    searchBarInputStyle?: any;
};

export const EmbeddedListSearchModal = ({
    emptySearchQuery,
    fixedFilter,
    fixedQuery,
    placeholderText,
    defaultShowFilters,
    defaultFilters,
    onClose,
    searchBarStyle,
    searchBarInputStyle,
}: Props) => {
    // Component state
    const [query, setQuery] = useState<string>('');
    const [page, setPage] = useState(1);
    const [filters, setFilters] = useState<Array<FacetFilterInput>>([]);

    const onChangeQuery = (q: string) => {
        setQuery(q);
    };

    const onChangeFilters = (newFilters: Array<FacetFilterInput>) => {
        setFilters(newFilters);
    };

    const onChangePage = (newPage: number) => {
        setPage(newPage);
    };

    return (
        <Modal
            width={800}
            style={{ top: 40 }}
            bodyStyle={{ padding: 0 }}
            title="View Ingested Assets"
            visible
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose}>Close</Button>
                </>
            }
        >
            <div style={{ height: 500 }}>
                <EmbeddedListSearch
                    query={query}
                    filters={filters}
                    page={page}
                    onChangeQuery={onChangeQuery}
                    onChangeFilters={onChangeFilters}
                    onChangePage={onChangePage}
                    emptySearchQuery={emptySearchQuery}
                    fixedFilter={fixedFilter}
                    fixedQuery={fixedQuery}
                    placeholderText={placeholderText}
                    defaultShowFilters={defaultShowFilters}
                    defaultFilters={defaultFilters}
                    searchBarStyle={searchBarStyle}
                    searchBarInputStyle={searchBarInputStyle}
                />
            </div>
        </Modal>
    );
};
