import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useVT } from 'virtualizedtableforantd4';

import { StyledTable } from '@app/entityV2/shared/tabs/Dataset/Validations/AcrylAssertionsTable';
import { useAssertionsTableColumns } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/hooks';
import { AssertionListFilter, AssertionTable } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/types';
import { getEntityUrnForAssertion, getSiblingWithUrn } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { useOpenAssertionDetailModal } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/hooks';
import { AssertionProfileDrawer } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/AssertionProfileDrawer';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { AssertionType, DataContract, Entity } from '@src/types.generated';

type Props = {
    assertionData: AssertionTable;
    filter: AssertionListFilter;
    refetch: () => void;
    contract: DataContract;
    canEditAssertions: boolean;
    canEditMonitors: boolean;
    canEditSqlAssertions: boolean;
};

export const AcrylAssertionListTable = ({
    assertionData,
    filter,
    refetch,
    contract,
    canEditAssertions,
    canEditMonitors,
    canEditSqlAssertions,
}: Props) => {
    const { entityData } = useEntityData();
    const { groupBy } = filter;

    // get columns data from the custom hooks
    const assertionsTableCols = useAssertionsTableColumns({
        groupBy,
        contract,
        canEditSqlAssertions,
        canEditAssertions,
        canEditMonitors,
        refetch,
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

    const [VT] = useVT(() => ({ scroll: { y: 400 } }), []);
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
        <>
            <StyledTable
                columns={assertionsTableCols as any}
                showSelect
                dataSource={memoizedData}
                showHeader
                components={VT}
                scroll={{ y: 400 }}
                pagination={{
                    pageSize: 25,
                }}
                rowClassName={rowClassName}
                bordered
                onRow={handleRowClick}
            />

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
        </>
    );
};
