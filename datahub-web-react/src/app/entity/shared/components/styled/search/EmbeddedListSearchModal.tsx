import { Button, Modal } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { EmbeddedListSearch } from '@app/entity/shared/components/styled/search/EmbeddedListSearch';
import { EntityActionProps } from '@app/entity/shared/components/styled/search/EntitySearchResults';
import { FilterSet } from '@app/entity/shared/components/styled/search/types';
import { UnionType } from '@app/search/utils/constants';

import { FacetFilterInput } from '@types';

const SearchContainer = styled.div`
    height: 500px;
`;
const modalStyle = {
    top: 40,
};

const modalBodyStyle = {
    padding: 0,
};

type Props = {
    title: React.ReactNode;
    emptySearchQuery?: string | null;
    fixedFilters?: FilterSet;
    fixedQuery?: string | null;
    placeholderText?: string | null;
    defaultShowFilters?: boolean;
    defaultFilters?: Array<FacetFilterInput>;
    onClose?: () => void;
    searchBarStyle?: any;
    searchBarInputStyle?: any;
    entityAction?: React.FC<EntityActionProps>;
    applyView?: boolean;
};

export const EmbeddedListSearchModal = ({
    title,
    emptySearchQuery,
    fixedFilters,
    fixedQuery,
    placeholderText,
    defaultShowFilters,
    defaultFilters,
    onClose,
    searchBarStyle,
    searchBarInputStyle,
    entityAction,
    applyView,
}: Props) => {
    // Component state
    const [query, setQuery] = useState<string>('');
    const [page, setPage] = useState(1);
    const [unionType, setUnionType] = useState(UnionType.AND);

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
            style={modalStyle}
            bodyStyle={modalBodyStyle}
            title={title}
            open
            onCancel={onClose}
            footer={<Button onClick={onClose}>Close</Button>}
        >
            <SearchContainer>
                <EmbeddedListSearch
                    query={query}
                    filters={filters}
                    page={page}
                    unionType={unionType}
                    onChangeQuery={onChangeQuery}
                    onChangeFilters={onChangeFilters}
                    onChangePage={onChangePage}
                    onChangeUnionType={setUnionType}
                    emptySearchQuery={emptySearchQuery}
                    fixedFilters={fixedFilters}
                    fixedQuery={fixedQuery}
                    placeholderText={placeholderText}
                    defaultShowFilters={defaultShowFilters}
                    defaultFilters={defaultFilters}
                    searchBarStyle={searchBarStyle}
                    searchBarInputStyle={searchBarInputStyle}
                    entityAction={entityAction}
                    applyView={applyView}
                />
            </SearchContainer>
        </Modal>
    );
};
