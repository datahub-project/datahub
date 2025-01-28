import React, { useState } from 'react';
import { Typography, Table, TablePaginationConfig } from 'antd';
import { Popover } from '@components';
import { TooltipPlacement } from 'antd/es/tooltip';
import { InfoCircleOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { FacetFilterInput } from '@src/types.generated';
import { QueriesTabSection, Query } from './types';
import { DEFAULT_PAGE_SIZE } from './utils/constants';
import { ANTD_GRAY, ANTD_GRAY_V2, REDESIGN_COLORS } from '../../../constants';
import useQueryTableColumns from './useQueryTableColumns';
import Loading from '../../../../../shared/Loading';
import usePagination, { Pagination } from '../../../../../sharedV2/pagination/usePagination';
import { Sorting } from '../../../../../sharedV2/sorting/useSorting';
import AddButton from './AddButton';
import QueryFilters from './QueryFilters/QueryFilters';

const SectionWrapper = styled.div<{ $borderRadiusBottom?: boolean }>`
    background-color: white;
    padding: 24px;
    box-shadow: 0px 0px 5px rgba(0, 0, 0, 0.08);
    ${(props) => (props.$borderRadiusBottom ? `border-radius: 0 0 10px 10px;` : `border-radius: 10px;`)}
    height: 100%;
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

const QueriesTitle = styled(Typography.Text)`
    && {
        margin: 0px;
        font-size: 16px;
        font-weight: 700;
        color: ${REDESIGN_COLORS.TEXT_HEADING};
    }
`;

const StyledInfoOutlined = styled(InfoCircleOutlined)`
    margin-left: 8px;
    font-size: 12px;
    color: ${ANTD_GRAY[7]};
`;

const StyledTable = styled(Table)`
    .ant-table-thead > tr > th {
        font-weight: 700;
        font-size: 14px;
        line-height: 16px;
        color: ${ANTD_GRAY_V2[12]};
    }

    .lastRun {
        min-width: 120px;
    }
    .usedBy {
        min-width: 100px;
    }
`;

const LoadingWrapper = styled.div`
    height: 300px;
    display: flex;
    align-items: center;
`;

const FiltersContainer = styled.div`
    display: flex;
    gap: 10px;
`;

type Props = {
    title: string;
    queries: Query[];
    totalQueries: number;
    tooltip?: string;
    tooltipPosition?: TooltipPlacement;
    initialPageSize?: number;
    showDetails?: boolean;
    showEdit?: boolean;
    showDelete?: boolean;
    onDeleted?: (query) => void;
    onEdited?: (query) => void;
    section: QueriesTabSection;
    selectedColumnsFilter: FacetFilterInput;
    setSelectedColumnsFilter: (usersFilter: FacetFilterInput) => void;
    selectedUsersFilter: FacetFilterInput;
    setSelectedUsersFilter: (usersFilter: FacetFilterInput) => void;
    loading?: boolean;
    pagination?: Pagination;
    sorting?: Sorting;
    addQueryDisabled?: boolean;
    onAddQuery?: () => void;
    isTopSection?: boolean;
};

export default function QueriesListSection({
    title,
    tooltip,
    tooltipPosition,
    queries,
    totalQueries,
    showDetails,
    showEdit,
    showDelete,
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
}: Props) {
    /**
     * Table state
     */
    const [hoveredQueryUrn, setHoveredQueryUrn] = useState<string | null>(null);
    const defaultPagination = usePagination(DEFAULT_PAGE_SIZE);
    const { pageSize, page, setPage } = pagination || defaultPagination;
    const showPagination = totalQueries > pageSize;

    const {
        titleColumn,
        descriptionColumn,
        queryTextColumn,
        createdByColumn,
        createdDateColumn,
        powersColumn,
        usedByColumn,
        popularityColumn,
        columnsColumn,
        lastRunColumn,
        editColumn,
    } = useQueryTableColumns({
        queries,
        hoveredQueryUrn,
        showDelete,
        showDetails,
        showEdit,
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

    const popularQueriesColumns = [queryTextColumn(), usedByColumn, lastRunColumn, columnsColumn, popularityColumn];

    const downstreamQueriesColumns = [queryTextColumn(550), powersColumn, lastRunColumn];

    const recentQueriesColumns = [queryTextColumn(550), lastRunColumn];

    const pagionationOptions: false | TablePaginationConfig = showPagination
        ? ({
              total: totalQueries,
              current: page,
              pageSize,
              position: ['bottomCenter'],
              onChange: (newPage: number) => {
                  setPage(newPage);
              },
          } as TablePaginationConfig)
        : false;

    const loadingConfig = loading
        ? {
              indicator: (
                  <LoadingWrapper>
                      <Loading />{' '}
                  </LoadingWrapper>
              ),
          }
        : false;

    const handleTableChange = (_pagination, _filters, tableSorting) => {
        if (showPagination && sorting && tableSorting && Object.keys(tableSorting).length) {
            sorting.setSortField((tableSorting as any).column?.field || null);
            sorting.setSortOrder((tableSorting as any).order || null);
        }
    };

    const tableProps = {
        dataSource: queries,
        pagination: pagionationOptions,
        style: loading ? { minHeight: 400 } : {},
        loading: loadingConfig,
        scroll: { x: 'auto' },
        onChange: handleTableChange,
    };

    return (
        <SectionWrapper $borderRadiusBottom={isTopSection}>
            <QueriesTitleSection>
                <TitleWrapper>
                    <QueriesTitle>{title}</QueriesTitle>
                    {tooltip && (
                        <Popover content={tooltip} placement={tooltipPosition}>
                            <StyledInfoOutlined />
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
                        buttonLabel="Add Highlighted Query"
                        isButtonDisabled={addQueryDisabled}
                        onButtonClick={onAddQuery}
                    />
                )}
            </QueriesTitleSection>
            {section === QueriesTabSection.Highlighted && (
                <StyledTable
                    {...tableProps}
                    columns={highlightedQueriesColumns}
                    onRow={(row) => {
                        return {
                            onMouseEnter: () => setHoveredQueryUrn((row as Query).urn || ''),
                            onMouseLeave: () => setHoveredQueryUrn(null),
                        };
                    }}
                />
            )}
            {section === QueriesTabSection.Popular && <StyledTable {...tableProps} columns={popularQueriesColumns} />}
            {section === QueriesTabSection.Downstream && (
                <StyledTable columns={downstreamQueriesColumns} {...tableProps} />
            )}
            {section === QueriesTabSection.Recent && <StyledTable columns={recentQueriesColumns} {...tableProps} />}
        </SectionWrapper>
    );
}
