import React, { useEffect, useState } from 'react';
import { DataContract } from '@src/types.generated';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { Table } from '@src/alchemy-components';
import { SortingState } from '@src/alchemy-components/components/Table/types';

import { AssertionProfileDrawer } from '../assertion/profile/AssertionProfileDrawer';
import { getEntityUrnForAssertion, getSiblingWithUrn } from '../acrylUtils';
import { useExpandedRowKeys, useOpenAssertionDetailModal } from '../assertion/builder/hooks';
import { AssertionTable, AssertionListFilter } from './types';
import { useAssertionsTableColumns } from './hooks';
import { StyledTableContainer } from './StyledComponents';

type Props = {
    assertionData: AssertionTable;
    filter: AssertionListFilter;
    refetch: () => void;
    contract: DataContract;
};

export const AcrylAssertionListTable = ({ assertionData, filter, refetch, contract }: Props) => {
    const { entityData } = useEntityData();
    const { groupBy } = filter;

    const [sortedOptions, setSortedOptions] = useState<{ sortColumn: string; sortOrder: SortingState }>({
        sortColumn: '',
        sortOrder: SortingState.ORIGINAL,
    });

    const { expandedRowKeys, setExpandedRowKeys } = useExpandedRowKeys(
        assertionData?.groupBy ? assertionData?.groupBy[groupBy] : [],
        { isGroupBy: !!groupBy },
    );

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

    const onAssertionExpand = (record) => {
        const key = record.name;
        setExpandedRowKeys((prev) => (prev.includes(key) ? prev.filter((k) => k !== key) : [...prev, key]));
    };

    const getGroupData = () => {
        return (assertionData?.groupBy && assertionData?.groupBy[groupBy]) || [];
    };

    const rowClassName = (record): string => {
        if (record.groupName) {
            return 'group-header';
        }
        if (record.urn === focusAssertionUrn) {
            return 'acryl-selected-table-row' || 'acryl-assertions-table-row';
        }
        return 'acryl-assertions-table-row';
    };

    const onRowClick = (record) => {
        setFocusAssertionUrn(record.urn);
    };

    const getSortedAssertions = (record) => {
        const { sortOrder, sortColumn } = sortedOptions;
        if (sortOrder === SortingState.ORIGINAL) {
            return record.assertions;
        }

        const sortFunctions = {
            lastEvaluation: {
                [SortingState.DESCENDING]: (a, b) => a.lastEvaluationTimeMs - b.lastEvaluationTimeMs,
                [SortingState.ASCENDING]: (a, b) => b.lastEvaluationTimeMs - a.lastEvaluationTimeMs,
            },
            name: {
                [SortingState.ASCENDING]: (a, b) => a.description.localeCompare(b.description),
                [SortingState.DESCENDING]: (a, b) => b.description.localeCompare(a.description),
            },
        };

        const sortFunction = sortFunctions[sortColumn]?.[sortOrder];
        return sortFunction ? [...record.assertions].sort(sortFunction) : record.assertions;
    };

    return (
        <>
            <StyledTableContainer style={{ height: '100vh', overflow: 'hidden' }}>
                <Table
                    columns={assertionsTableCols}
                    data={groupBy ? getGroupData() : assertionData.assertions || []}
                    showHeader
                    isScrollable
                    rowClassName={rowClassName}
                    handleSortColumnChange={({
                        sortColumn,
                        sortOrder,
                    }: {
                        sortColumn: string;
                        sortOrder: SortingState;
                    }) => setSortedOptions({ sortColumn, sortOrder })}
                    expandable={{
                        expandedRowRender: (record) => {
                            let sortedAssertions = record.assertions;
                            if (sortedOptions.sortColumn && sortedOptions.sortOrder) {
                                sortedAssertions = getSortedAssertions(record);
                            }
                            return (
                                <Table
                                    columns={assertionsTableCols}
                                    data={sortedAssertions}
                                    showHeader={false}
                                    isBorderless
                                    isExpandedInnerTable
                                    onRowClick={onRowClick}
                                    rowClassName={rowClassName}
                                />
                            );
                        },
                        rowExpandable: () => !!groupBy,
                        expandIconPosition: 'end',
                        expandedRowKeys,
                    }}
                    onExpand={onAssertionExpand}
                />
            </StyledTableContainer>
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
