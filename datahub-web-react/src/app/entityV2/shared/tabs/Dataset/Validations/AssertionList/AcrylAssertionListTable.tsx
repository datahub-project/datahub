import ResizeObserver from 'rc-resize-observer';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import { StyledTable } from '@app/entityV2/shared/tabs/Dataset/Validations/AcrylAssertionsTable';
import { useAssertionsTableColumns } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/hooks';
import { AssertionTable } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/types';
import { getEntityUrnForAssertion, getSiblingWithUrn } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
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
    assertionData: AssertionTable;
    refetch: () => void;
    contract: DataContract;
    canEditAssertions: boolean;
    canEditMonitors: boolean;
    canEditSqlAssertions: boolean;
    isEntityReachable: boolean;
};

export const AcrylAssertionListTable = ({
    assertionData,
    refetch,
    contract,
    canEditAssertions,
    canEditMonitors,
    canEditSqlAssertions,
    isEntityReachable,
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
    const focusedAssertion = assertionData.assertions.find((assertion) => assertion.urn === focusAssertionUrn);
    const focusedEntityUrn = focusedAssertion ? getEntityUrnForAssertion(focusedAssertion.assertion) : undefined;

    const focusedAssertionEntity =
        focusedEntityUrn && entityData ? getSiblingWithUrn(entityData, focusedEntityUrn) : undefined;

    const canEditFocusAssertion = focusedAssertion
        ? (focusedAssertion?.type === AssertionType.Sql && canEditSqlAssertions) || canEditAssertions
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

    const memoizedData = useMemo(
        () => assertionData.assertions.map((assertion) => ({ ...assertion, key: assertion.urn })),
        [assertionData.assertions],
    );

    const handleRowClick = useCallback(
        (record) => {
            return {
                onClick: () => {
                    setFocusAssertionUrn(record.urn);
                },
            };
        },
        [setFocusAssertionUrn],
    ); // Only recreate if setFocusAssertionUrn changes

    return (
        <TableContainer>
            <ResizeObserver
                onResize={(dimensions) => setTableHeight(dimensions.height - HEADER_AND_PAGINATION_HEIGHT_PX)}
            >
                <StyledTable
                    columns={assertionsTableCols as any}
                    showSelect
                    dataSource={memoizedData}
                    showHeader
                    scroll={{
                        y: tableHeight,
                    }}
                    pagination={{
                        pageSize: 50,
                        position: ['bottomCenter'],
                        showSizeChanger: false,
                    }}
                    rowClassName={rowClassName}
                    bordered={false}
                    onRow={handleRowClick}
                    tableLayout="fixed"
                />
            </ResizeObserver>

            {focusAssertionUrn && focusedAssertionEntity && (
                <AssertionProfileDrawer
                    urn={focusAssertionUrn}
                    entity={focusedAssertionEntity as Entity}
                    contract={contract}
                    canEditAssertion={canEditFocusAssertion}
                    canEditMonitor={canEditFocusMonitor}
                    closeDrawer={() => setFocusAssertionUrn(null)}
                    refetch={refetch}
                />
            )}
        </TableContainer>
    );
};
