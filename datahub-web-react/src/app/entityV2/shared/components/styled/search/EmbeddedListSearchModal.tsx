import React, { useState } from 'react';
import { Button, Modal } from 'antd';
import styled from 'styled-components';
import {
    AndFilterInput,
    EntityType,
    FacetFilterInput,
    SearchFlags,
    SortCriterion,
} from '../../../../../../types.generated';
import { EmbeddedListSearch } from './EmbeddedListSearch';
import { UnionType } from '../../../../../search/utils/constants';
import { FilterSet } from './types';
import { EntityActionProps } from './EntitySearchResults';

const SearchContainer = styled.div<{ height?: string }>`
    height: ${(props) => props.height || '500px'};
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
    fixedOrFilters?: AndFilterInput[];
    fixedQuery?: string | null;
    placeholderText?: string | null;
    defaultShowFilters?: boolean;
    defaultFilters?: Array<FacetFilterInput>;
    onClose?: () => void;
    searchBarStyle?: any;
    searchBarInputStyle?: any;
    entityAction?: React.FC<EntityActionProps>;
    applyView?: boolean;
    height?: string;
    sort?: SortCriterion;
    entityTypes?: EntityType[];
    searchFlags?: SearchFlags;
};

export const EmbeddedListSearchModal = ({
    title,
    emptySearchQuery,
    fixedFilters,
    fixedOrFilters,
    fixedQuery,
    placeholderText,
    defaultShowFilters,
    defaultFilters,
    onClose,
    searchBarStyle,
    searchBarInputStyle,
    entityAction,
    applyView,
    height,
    sort,
    entityTypes,
    searchFlags,
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
            visible
            onCancel={onClose}
            footer={<Button onClick={onClose}>Close</Button>}
        >
            <SearchContainer height={height}>
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
                    fixedOrFilters={fixedOrFilters}
                    fixedQuery={fixedQuery}
                    placeholderText={placeholderText}
                    defaultShowFilters={defaultShowFilters}
                    defaultFilters={defaultFilters}
                    searchBarStyle={searchBarStyle}
                    searchBarInputStyle={searchBarInputStyle}
                    entityAction={entityAction}
                    applyView={applyView}
                    sort={sort}
                    entityTypes={entityTypes}
                    searchFlags={searchFlags}
                />
            </SearchContainer>
        </Modal>
    );
};
