import { Pagination, Popover, Table, Text } from '@components';
import { Info } from '@phosphor-icons/react/dist/csr/Info';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { Column, SortingState } from '@components/components/Table/types';

import AddButton from '@app/entityV2/shared/tabs/Dataset/Queries/AddButton';
import QueryFilters from '@app/entityV2/shared/tabs/Dataset/Queries/QueryFilters/QueryFilters';
import { QueriesTabSection, Query } from '@app/entityV2/shared/tabs/Dataset/Queries/types';
import useQueryTableColumns from '@app/entityV2/shared/tabs/Dataset/Queries/useQueryTableColumns';
import { DEFAULT_PAGE_SIZE } from '@app/entityV2/shared/tabs/Dataset/Queries/utils/constants';
import {
    getQueriesTableData,
    shouldShowQueriesTableLoading,
} from '@app/entityV2/shared/tabs/Dataset/Queries/utils/getQueriesTableData';
import usePagination, { Pagination as PaginationState } from '@app/sharedV2/pagination/usePagination';
import { Sorting } from '@app/sharedV2/sorting/useSorting';
import { FacetFilterInput } from '@src/types.generated';

type TooltipPlacement = React.ComponentProps<typeof Popover>['placement'];

const POPULAR_ROW_TEST_ID = 'popular-query-row';
const PAGE_SIZE_OPTIONS = [5, 10, 20, 50, 100];
const SMALLEST_PAGE_SIZE = Math.min(...PAGE_SIZE_OPTIONS);

const SectionWrapper = styled.div<{ $borderRadiusBottom?: boolean; $fillHeight?: boolean }>`
    background-color: ${(props) => props.theme.colors.bg};
    padding: 24px;
    box-shadow: ${(props) => props.theme.colors.shadowXs};
    ${(props) => (props.$borderRadiusBottom ? `border-radius: 0 0 10px 10px;` : `border-radius: 10px;`)}
    display: flex;
    flex-direction: column;
    /* The bottom-most section stretches to the end of the tab so the card lines up with the
       entity sidebar. Sections above it stay content-sized. */
    flex: ${(props) => (props.$fillHeight ? '1 0 auto' : '0 0 auto')};
    ${(props) => props.$fillHeight && `max-height: 100%;`}
    min-height: 0;
    overflow: hidden;
`;

const QueriesTitleSection = styled.div`
    display: flex;
    align-items: center;
    margin-bottom: 20px;
    justify-content: space-between;
`;

const TitleWrapper = styled.div`
    display: flex;
    align-items: center;
`;

const QueriesTitle = styled(Text)`
    margin: 0;
`;

const StyledInfo = styled(Info)`
    margin-left: 8px;
    color: ${(props) => props.theme.colors.textTertiary};
`;

const FiltersContainer = styled.div`
    display: flex;
    gap: 10px;
`;

const TableWrapper = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    min-height: 0;
    overflow: hidden;
`;

const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
    padding-top: 8px;
`;

type Props = {
    title: string;
    queries: Query[];
    totalQueries: number;
    tooltip?: string;
    tooltipPosition?: TooltipPlacement;
    showDetails?: boolean;
    onDeleted?: (query) => void;
    onEdited?: (query) => void;
    section: QueriesTabSection;
    selectedColumnsFilter: FacetFilterInput;
    setSelectedColumnsFilter: (usersFilter: FacetFilterInput) => void;
    selectedUsersFilter: FacetFilterInput;
    setSelectedUsersFilter: (usersFilter: FacetFilterInput) => void;
    loading?: boolean;
    pagination?: PaginationState;
    sorting?: Sorting;
    addQueryDisabled?: boolean;
    onAddQuery?: () => void;
    isTopSection?: boolean;
    fillHeight?: boolean;
};

export default function QueriesListSection({
    title,
    tooltip,
    tooltipPosition,
    queries,
    totalQueries,
    showDetails,
    onDeleted,
    onEdited,
    section,
    selectedColumnsFilter,
    setSelectedColumnsFilter,
    selectedUsersFilter,
    setSelectedUsersFilter,
    loading,
    pagination,
    sorting,
    addQueryDisabled,
    onAddQuery,
    isTopSection,
    fillHeight,
}: Props) {
    const { t } = useTranslation('entity.profile.queries');
    const defaultPagination = usePagination(DEFAULT_PAGE_SIZE);
    // Popular / Highlighted pass a server-backed pagination hook; Downstream / Recent omit it and
    // fall back to local page state. Alchemy's Table does not slice rows itself.
    const isServerPaginated = pagination != null;
    const { pageSize, page, setPage, setPageSize } = pagination || defaultPagination;
    const tableData = getQueriesTableData({
        queries,
        isServerPaginated,
        page,
        pageSize,
    });
    const showPagination = totalQueries > pageSize;
    // Visibility is independent of the current page size so choosing a size that fits every row
    // does not hide the control needed to switch back.
    const showPaginationControls = totalQueries > SMALLEST_PAGE_SIZE;

    const {
        titleColumn,
        descriptionColumn,
        queryTextColumn,
        createdByColumn,
        createdDateColumn,
        powersColumn,
        topUsersColumn,
        columnsColumn,
        editColumn,
    } = useQueryTableColumns({
        showDetails,
        onDeleted,
        onEdited,
        sorting,
        showPagination,
    });

    const highlightedQueriesColumns = [
        titleColumn,
        descriptionColumn,
        queryTextColumn(),
        createdByColumn,
        createdDateColumn,
        editColumn,
    ];

    const popularQueriesColumns = [queryTextColumn(), topUsersColumn, columnsColumn];

    const downstreamQueriesColumns = [queryTextColumn('550px'), powersColumn];

    const recentQueriesColumns = [queryTextColumn('550px')];

    const handleSortColumnChange = ({ sortColumn, sortOrder }: { sortColumn: string; sortOrder: SortingState }) => {
        if (!showPagination || !sorting) return;

        const sortFields: Record<string, string> = {
            name: 'name',
            dateCreated: 'createdAt',
        };
        sorting.setSortField(sortFields[sortColumn] ?? null);
        sorting.setSortOrder(sortOrder === SortingState.ORIGINAL ? null : sortOrder);
    };

    const renderTable = (columns: Column<Query>[], rowDataTestId?: () => string) => (
        <Table
            columns={columns}
            data={tableData}
            isLoading={shouldShowQueriesTableLoading(loading, tableData.length)}
            isScrollable
            rowKey={(query) => query.urn || query.query}
            rowDataTestId={rowDataTestId}
            handleSortColumnChange={handleSortColumnChange}
            data-testid={`queries-table-${section}`}
        />
    );

    return (
        <SectionWrapper
            $borderRadiusBottom={isTopSection}
            $fillHeight={fillHeight}
            data-testid={`queries-list-section-${section}`}
        >
            <QueriesTitleSection>
                <TitleWrapper>
                    <QueriesTitle type="span" size="lg" weight="bold">
                        {title}
                    </QueriesTitle>
                    {tooltip && (
                        <Popover content={tooltip} placement={tooltipPosition}>
                            <StyledInfo size={12} />
                        </Popover>
                    )}
                </TitleWrapper>
                {section === QueriesTabSection.Popular && (
                    <FiltersContainer>
                        <QueryFilters
                            selectedUsersFilter={selectedUsersFilter}
                            setSelectedUsersFilter={setSelectedUsersFilter}
                            selectedColumnsFilter={selectedColumnsFilter}
                            setSelectedColumnsFilter={setSelectedColumnsFilter}
                            setPage={setPage}
                        />
                    </FiltersContainer>
                )}
                {section === QueriesTabSection.Highlighted && (
                    <AddButton
                        dataTestId="add-query-button"
                        buttonLabel={t('queriesTab.addHighlightedQueryButton')}
                        isButtonDisabled={addQueryDisabled}
                        onButtonClick={onAddQuery}
                    />
                )}
            </QueriesTitleSection>
            <TableWrapper>
                {section === QueriesTabSection.Highlighted && renderTable(highlightedQueriesColumns)}
                {section === QueriesTabSection.Popular && renderTable(popularQueriesColumns, () => POPULAR_ROW_TEST_ID)}
                {section === QueriesTabSection.Downstream && renderTable(downstreamQueriesColumns)}
                {section === QueriesTabSection.Recent && renderTable(recentQueriesColumns)}
            </TableWrapper>
            {showPaginationControls && (
                <PaginationContainer>
                    <Pagination
                        currentPage={page}
                        itemsPerPage={pageSize}
                        total={totalQueries}
                        pageSizeOptions={PAGE_SIZE_OPTIONS}
                        showSizeChanger
                        onPageChange={(newPage) => setPage(newPage)}
                        onShowSizeChange={(newPage, newPageSize) => {
                            setPage(newPage);
                            setPageSize(newPageSize);
                        }}
                    />
                </PaginationContainer>
            )}
        </SectionWrapper>
    );
}
