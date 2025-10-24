import { TableProps } from 'antd';
import { SorterResult } from 'antd/es/table/interface';
import ResizeObserver from 'rc-resize-observer';
import React, { useCallback, useEffect, useState } from 'react';
import styled from 'styled-components';

import { StyledTable } from '@app/entityV2/shared/tabs/Dataset/Validations/AcrylAssertionsTable';
import { useAssertionsTableColumns } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/hooks';
import { AssertionListTableRow } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/types';
import { mapAssertionDataToTableProperties } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/utils';
import {
    AssertionWithMonitorDetails,
    getEntityUrnForAssertion,
    getSiblingWithUrn,
} from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { useOpenAssertionDetailModal } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/hooks';
import { AssertionProfileDrawer } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/AssertionProfileDrawer';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { AssertionType, DataContract, Entity } from '@src/types.generated';

const HEADER_AND_PAGINATION_HEIGHT_PX = 104;

const TableContainer = styled.div`
    overflow: hidden;
    height: 100%;
    max-height: 100%;
`;

type Props = {
    assertions: AssertionWithMonitorDetails[];
    refetch: () => void;
    contract: DataContract | undefined;
    canEditAssertions: boolean;
    canEditMonitors: boolean;
    canEditSqlAssertions: boolean;
    isEntityReachable: boolean;
    page: number;
    setPage: (page: number) => void;
    pageSize: number;
    totalAssertions: number;
    loading: boolean;
    onSortColumnChange: (sorter: SorterResult<AssertionListTableRow>) => void;
};

export const AcrylAssertionListTable = ({
    assertions,
    refetch,
    contract,
    canEditAssertions,
    canEditMonitors,
    canEditSqlAssertions,
    isEntityReachable,
    page,
    setPage,
    pageSize,
    totalAssertions,
    loading,
    onSortColumnChange,
}: Props) => {
    const { entityData } = useEntityData();
    const [tableHeight, setTableHeight] = useState(0);

    // get columns data from the custom hooks
    const assertionsTableCols = useAssertionsTableColumns({
        contract,
        canEditSqlAssertions,
        canEditAssertions,
        canEditMonitors,
        refetch,
        isEntityReachable,
    });

    const [focusAssertionUrn, setFocusAssertionUrn] = useState<string | null>(null);
    const focusedAssertion = assertions.find((assertion) => assertion.urn === focusAssertionUrn);
    const focusedEntityUrn = focusedAssertion ? getEntityUrnForAssertion(focusedAssertion) : undefined;

    const focusedAssertionEntity =
        focusedEntityUrn && entityData ? getSiblingWithUrn(entityData, focusedEntityUrn) : undefined;
    const canEditFocusAssertion = focusedAssertion
        ? (focusedAssertion?.info?.type === AssertionType.Sql && canEditSqlAssertions) || canEditAssertions
        : false;
    const canEditFocusMonitor = focusedAssertion ? canEditMonitors : false;

    useEffect(() => {
        if (focusAssertionUrn && !focusedAssertion) {
            setFocusAssertionUrn(null);
        }
    }, [focusAssertionUrn, focusedAssertion]);

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

    const assertionRows = mapAssertionDataToTableProperties(assertions);

    const handleRowClick = useCallback(
        (record) => {
            return {
                onClick: () => {
                    setFocusAssertionUrn(record.urn);
                },
            };
        },
        [setFocusAssertionUrn],
    );

    const handleTableChange: TableProps<AssertionListTableRow>['onChange'] = (_, __, sorter) => {
        if (sorter && Object.keys(sorter).length && !Array.isArray(sorter)) {
            onSortColumnChange(sorter as SorterResult<AssertionListTableRow>);
        }
    };

    return (
        <TableContainer>
            <ResizeObserver
                onResize={(dimensions) => setTableHeight(dimensions.height - HEADER_AND_PAGINATION_HEIGHT_PX)}
            >
                <StyledTable<AssertionListTableRow>
                    columns={assertionsTableCols}
                    showSelect
                    dataSource={assertionRows}
                    showHeader
                    scroll={{
                        y: tableHeight,
                    }}
                    pagination={{
                        current: page,
                        pageSize,
                        total: totalAssertions,
                        position: ['bottomCenter'],
                        showSizeChanger: false,
                        onChange: (newPage) => {
                            setPage(newPage);
                        },
                    }}
                    rowClassName={rowClassName}
                    bordered={false}
                    onRow={handleRowClick}
                    tableLayout="fixed"
                    loading={loading}
                    onChange={handleTableChange}
                />
            </ResizeObserver>
            {focusAssertionUrn && focusedAssertionEntity && (
                <AssertionProfileDrawer
                    urn={focusAssertionUrn}
                    entity={focusedAssertionEntity as Entity}
                    canEditAssertion={canEditFocusAssertion}
                    canEditMonitor={canEditFocusMonitor}
                    closeDrawer={() => setFocusAssertionUrn(null)}
                />
            )}
        </TableContainer>
    );
};
