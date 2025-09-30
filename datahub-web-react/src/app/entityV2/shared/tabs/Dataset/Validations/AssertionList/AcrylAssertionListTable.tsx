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
import { DataContract } from '@src/types.generated';

type Props = {
    assertionData: AssertionTable;
    refetch: () => void;
    contract: DataContract;
};

const HEADER_AND_PAGINATION_HEIGHT_PX = 130;

const TableContainer = styled.div`
    overflow: hidden;
    height: 100%;
    max-height: 100%;
`;

export const AcrylAssertionListTable = ({ assertionData, refetch, contract }: Props) => {
    const { entityData } = useEntityData();
    const [tableHeight, setTableHeight] = useState(0);

    // get columns data from the custom hooks
    const assertionsTableCols = useAssertionsTableColumns({
        contract,
        refetch,
    });

    const [focusAssertionUrn, setFocusAssertionUrn] = useState<string | null>(null);
    const focusedAssertion = assertionData.assertions.find((assertion) => assertion.urn === focusAssertionUrn);
    const focusedEntityUrn = focusedAssertion ? getEntityUrnForAssertion(focusedAssertion.assertion) : undefined;

    const focusedAssertionEntity =
        focusedEntityUrn && entityData ? getSiblingWithUrn(entityData, focusedEntityUrn) : undefined;

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
    );

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
                        x: 'max-content',
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
                    contract={contract}
                    closeDrawer={() => setFocusAssertionUrn(null)}
                    refetch={refetch}
                />
            )}
        </TableContainer>
    );
};
