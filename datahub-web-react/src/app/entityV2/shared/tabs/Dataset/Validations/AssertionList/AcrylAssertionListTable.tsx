import ResizeObserver from 'rc-resize-observer';
import React, { useCallback, useState } from 'react';
import styled from 'styled-components';

import { Pagination } from '@components/components/Pagination';
import { Table } from '@components/components/Table';
import { SortingState } from '@components/components/Table/types';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { useAssertionsTableColumns } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/hooks';
import { AssertionListTableRow } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/types';
import { mapAssertionDataToTableProperties } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/utils';
import { AssertionWithMonitorDetails } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { useOpenAssertionDetailModal } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/hooks';
import { AssertionProfileDrawer } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/AssertionProfileDrawer';
import { DataContract } from '@src/types.generated';

type Props = {
    assertions: AssertionWithMonitorDetails[];
    refetch: () => void;
    contract: DataContract | undefined;
    isEntityReachable: boolean;
    page: number;
    setPage: (page: number) => void;
    pageSize: number;
    totalAssertions: number;
    loading: boolean;
    entityData: GenericEntityProperties;
    onSortColumnChange: (sorter: { sortColumn: string; sortOrder: SortingState }) => void;
};

const HEADER_AND_PAGINATION_HEIGHT_PX = 46;

const TableContainer = styled.div`
    overflow: hidden;
    height: 100%;
    max-height: 100%;
`;

const TableWrapper = styled.div`
    display: flex;
    flex-direction: column;
    height: 100%;
    .acryl-assertion-list-table-pagination {
        margin-bottom: 0;
    }
`;

export const AcrylAssertionListTable = ({
    assertions,
    refetch,
    contract,
    isEntityReachable: _isEntityReachable,
    page,
    setPage,
    pageSize,
    totalAssertions,
    loading,
    entityData,
    onSortColumnChange,
}: Props) => {
    const [tableHeight, setTableHeight] = useState(0);

    // get columns data from the custom hooks
    const assertionsTableCols = useAssertionsTableColumns({
        contract,
        refetch,
    });

    const [focusAssertionUrn, setFocusAssertionUrn] = useState<string | null>(null);
    const assertionRows = mapAssertionDataToTableProperties(assertions);

    useOpenAssertionDetailModal(setFocusAssertionUrn);

    const rowClassName = (record): string => {
        if (record.groupName) {
            return 'group-header';
        }
        if (record.urn === focusAssertionUrn) {
            return 'acryl-selected-table-row';
        }
        return 'acryl-assertions-table-row';
    };

    const handleRowClick = useCallback((record: AssertionListTableRow) => {
        setFocusAssertionUrn(record.urn);
    }, []);

    const handleSortColumnChange = useCallback(
        ({ sortColumn, sortOrder }: { sortColumn: string; sortOrder: SortingState }) => {
            onSortColumnChange({ sortColumn, sortOrder });
        },
        [onSortColumnChange],
    );

    return (
        <TableContainer>
            <ResizeObserver
                onResize={(dimensions) => setTableHeight(dimensions.height - HEADER_AND_PAGINATION_HEIGHT_PX)}
            >
                <TableWrapper>
                    <Table<AssertionListTableRow>
                        columns={assertionsTableCols}
                        data={assertionRows}
                        showHeader
                        isLoading={loading}
                        isScrollable
                        maxHeight={`${tableHeight}px`}
                        onRowClick={handleRowClick}
                        rowClassName={rowClassName}
                        handleSortColumnChange={handleSortColumnChange}
                        rowKey="urn"
                    />
                    <Pagination
                        currentPage={page}
                        itemsPerPage={pageSize}
                        total={totalAssertions}
                        onPageChange={(newPage) => {
                            setPage(newPage);
                        }}
                        loading={loading}
                        className="acryl-assertion-list-table-pagination"
                    />
                </TableWrapper>
            </ResizeObserver>
            {focusAssertionUrn && (
                <AssertionProfileDrawer
                    urn={focusAssertionUrn}
                    entity={entityData}
                    canEditAssertions={entityData?.privileges?.canEditAssertions || false}
                    closeDrawer={() => setFocusAssertionUrn(null)}
                />
            )}
        </TableContainer>
    );
};
