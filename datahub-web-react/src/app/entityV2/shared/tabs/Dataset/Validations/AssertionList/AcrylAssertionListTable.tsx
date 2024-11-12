import React, { useEffect, useState } from 'react';
import { Empty } from 'antd';
import { AssertionType, DataContract, Entity } from '@src/types.generated';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { DownOutlined } from '@ant-design/icons';
import styled from 'styled-components';

import { AssertionProfileDrawer } from '../assertion/profile/AssertionProfileDrawer';
import { getEntityUrnForAssertion, getSiblingWithUrn } from '../acrylUtils';
import { useExpandedRowKeys, useOpenAssertionDetailModal } from '../assertion/builder/hooks';
import { AssertionTable, AssertionListFilter } from './types';
import { useAssertionsTableColumns, usePinnedAssertionTableHeaderProps } from './hooks';
import { AssertionListStyledTable } from './StyledComponents';
import { GroupByTable } from './GroupByTable';

type Props = {
    assertionData: AssertionTable;
    filter: AssertionListFilter;
    refetch: () => void;
    contract: DataContract;
    canEditAssertions: boolean;
    canEditMonitors: boolean;
    canEditSqlAssertions: boolean;
};

const ExpandIcon = styled(DownOutlined)<{ expanded: boolean }>`
    transition: transform 0.3s ease-in-out;
    transform: rotate(${(props) => (props.expanded ? 180 : 0)}deg);
`;

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

    const { expandedRowKeys, setExpandedRowKeys } = useExpandedRowKeys(
        assertionData?.groupBy ? assertionData?.groupBy[groupBy] : [],
        { isGroupBy: !!groupBy },
    );

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

    const onAssertionExpand = (_, record) => {
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
            return 'acryl-selected-assertions-table-row' || 'acryl-assertions-table-row';
        }
        return 'acryl-assertions-table-row';
    };

    const onRowClick = (record) => {
        return {
            onClick: !record.groupName
                ? (_) => {
                      setFocusAssertionUrn(record.urn);
                  }
                : () => null,
        };
    };

    // get dynamic height for table row data to scroll which help to stick the table header
    const { tableContainerRef, scrollY } = usePinnedAssertionTableHeaderProps();

    return (
        <>
            <div ref={tableContainerRef} style={{ height: '100vh', overflow: 'hidden' }}>
                <AssertionListStyledTable
                    columns={assertionsTableCols}
                    dataSource={groupBy ? getGroupData() : assertionData.assertions || []}
                    locale={{
                        emptyText: <Empty description="No Assertions Found :(" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                    }}
                    showHeader
                    pagination={false}
                    rowClassName={rowClassName}
                    rowKey="name"
                    expandable={
                        groupBy
                            ? {
                                  expandedRowRender: (record: any) => (
                                      <GroupByTable
                                          groupData={record.assertions}
                                          columns={assertionsTableCols}
                                          onRow={onRowClick}
                                          focusUrn={focusAssertionUrn || ''}
                                          entityType="assertion"
                                      />
                                  ),
                                  onExpand: onAssertionExpand,
                                  expandedRowKeys,
                                  expandRowByClick: true,
                                  expandIcon: (props: any) => {
                                      const handleClick = (e) => {
                                          e.stopPropagation();
                                          props.onExpand(props.record, e);
                                      };

                                      return <ExpandIcon expanded={props.expanded} onClick={handleClick} />;
                                  },
                              }
                            : undefined
                    }
                    onRow={onRowClick}
                    scroll={{ y: scrollY }} // Dynamic scroll height
                />
            </div>
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
