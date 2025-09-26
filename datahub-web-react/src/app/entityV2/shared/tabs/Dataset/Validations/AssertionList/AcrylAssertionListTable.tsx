import React, { useEffect, useState } from 'react';

import { StyledTable } from '@app/entityV2/shared/tabs/Dataset/Validations/AcrylAssertionsTable';
import { useAssertionsTableColumns } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/hooks';
import { AssertionListFilter, AssertionTable } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/types';
import { getEntityUrnForAssertion, getSiblingWithUrn } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import { useOpenAssertionDetailModal } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/hooks';
import { AssertionProfileDrawer } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/AssertionProfileDrawer';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { DataContract } from '@src/types.generated';

type Props = {
    assertionData: AssertionTable;
    filter: AssertionListFilter;
    refetch: () => void;
    contract: DataContract;
};

export const AcrylAssertionListTable = ({ assertionData, filter, refetch, contract }: Props) => {
    const { entityData } = useEntityData();
    const { groupBy } = filter;

    // get columns data from the custom hooks
    const assertionsTableCols = useAssertionsTableColumns({
        groupBy,
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

    return (
        <>
            <StyledTable
                columns={assertionsTableCols as any}
                showSelect
                dataSource={assertionData.assertions}
                showHeader
                pagination={{
                    pageSize: 50,
                }}
                rowClassName={rowClassName}
                bordered
                onRow={(record) => {
                    return {
                        onClick: (_) => {
                            setFocusAssertionUrn(record.urn);
                        },
                    };
                }}
            />
            {focusAssertionUrn && focusedAssertionEntity && (
                <AssertionProfileDrawer
                    urn={focusAssertionUrn}
                    contract={contract}
                    closeDrawer={() => setFocusAssertionUrn(null)}
                    refetch={refetch}
                />
            )}
        </>
    );
};
