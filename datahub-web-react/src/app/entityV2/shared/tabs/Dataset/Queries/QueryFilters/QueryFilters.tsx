import React from 'react';
import styled from 'styled-components';
import { FilterPredicate } from '../../../../../../searchV2/filters/types';
import { convertToAvailableFilterPredictes } from '../../../../../../searchV2/filters/utils';
import { FacetFilterInput } from '../../../../../../../types.generated';
import SearchFilter from '../../../../../../searchV2/filters/SearchFilter';
import SelectedSearchFilters from '../../../../../../searchV2/filters/SelectedSearchFilters';
import { UnionType } from '../../../../../../searchV2/utils/constants';
import { ANTD_GRAY_V2, REDESIGN_COLORS } from '../../../../constants';
import useColumnsFilter from './useColumnsFilter';

const ColumnsFilterWrapper = styled.div`
    align-items: flex-end;
    display: flex;
    flex-direction: column;
    gap: 6px;
`;

const FiltersWrapper = styled.div`
    display: flex;
    gap: 8px;
`;

interface Props {
    selectedColumnsFilter: FacetFilterInput;
    setSelectedColumnsFilter: (columnsFilter: FacetFilterInput) => void;
    selectedUsersFilter: FacetFilterInput;
    setSelectedUsersFilter: (usersFilter: FacetFilterInput) => void;
    setPage: (c: number) => void;
}

export default function QueryFilters({
    selectedColumnsFilter,
    setSelectedColumnsFilter,
    setSelectedUsersFilter, // eslint-disable-line @typescript-eslint/no-unused-vars
    selectedUsersFilter,
    setPage,
}: Props) {
    const onChangeFilters = (newFilters: FacetFilterInput[]) => {
        const columnsFilter = newFilters.find((f) => f.field === 'entities');
        if (columnsFilter) {
            setSelectedColumnsFilter(columnsFilter);
        } else {
            setSelectedColumnsFilter({ field: 'entities', values: [] });
        }

        setPage(1);
    };

    const columnsFilter = useColumnsFilter({ selectedColumnsFilter, selectedUsersFilter, setSelectedColumnsFilter });

    const filterPredicates: FilterPredicate[] = convertToAvailableFilterPredictes(
        [selectedUsersFilter, selectedColumnsFilter],
        [columnsFilter],
    );
    const selectedFilters: FacetFilterInput[] = selectedColumnsFilter.values?.length ? [selectedColumnsFilter] : [];

    const labelStyle = {
        backgroundColor: ANTD_GRAY_V2[15],
        border: `1px solid ${REDESIGN_COLORS.COLD_GREY_TEXT_BLUE_1}`,
    };

    return (
        <ColumnsFilterWrapper>
            <FiltersWrapper>
                <SearchFilter
                    filter={columnsFilter}
                    filterPredicates={filterPredicates}
                    onChangeFilters={onChangeFilters}
                    activeFilters={[selectedColumnsFilter]}
                    labelStyle={selectedColumnsFilter.values?.length ? undefined : labelStyle}
                />
            </FiltersWrapper>
            <SelectedSearchFilters
                availableFilters={[columnsFilter]}
                selectedFilters={selectedFilters}
                unionType={UnionType.AND}
                onChangeFilters={onChangeFilters}
                onChangeUnionType={() => {}}
                onClearFilters={() => {}}
                showUnionType={false}
                showAddFilter={false}
                showClearAll={false}
                isCompact
                isOperatorDisabled
            />
        </ColumnsFilterWrapper>
    );
}
