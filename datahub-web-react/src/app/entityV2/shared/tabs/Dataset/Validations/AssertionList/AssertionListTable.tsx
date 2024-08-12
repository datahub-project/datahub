import React, { useState } from 'react';
import { Table, Empty } from 'antd';
import { AssertionType, DataContract, Entity } from '@src/types.generated';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { RightOutlined } from '@ant-design/icons';
import styled from 'styled-components';

import { AssertionProfileDrawer } from '../assertion/profile/AssertionProfileDrawer';
import { getEntityUrnForAssertion, getSiblingWithUrn } from '../acrylUtils';
import { useExpandedRowKeys, useOpenAssertionDetailModal } from '../assertion/builder/hooks';
import { AssertionTableType, IFilter } from './types';
import { useAssertionsTableColumns, usePinnedTableHeaderProps } from './hooks';
import { AssertionListStyledTable } from './StyledComponents';

type Props = {
    assertionData: AssertionTableType;
    filter: IFilter;
    refetch: () => void;
    contract: DataContract;
    canEditAssertions: boolean;
    canEditMonitors: boolean;
    canEditSqlAssertions: boolean;
};

const ExpandIcon = styled(RightOutlined)<{ expanded: boolean }>`
    transition: transform 0.3s ease-in-out;
    transform: rotate(${(props) => (props.expanded ? 90 : 0)}deg);
`;

export const AssertionListTable = ({
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
        groupBy,
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

    if (focusAssertionUrn && !focusedAssertion) {
        setFocusAssertionUrn(null);
    }
    useOpenAssertionDetailModal(setFocusAssertionUrn);

    const onAssertionExpand = (_, record) => {
        const key = record.name;
        setExpandedRowKeys((prev) => (prev.includes(key) ? prev.filter((k) => k !== key) : [...prev, key]));
    };

    const getGroupData = () => {
        return (assertionData?.groupBy && assertionData?.groupBy[groupBy]) || [];
    };

    const rowClassName = (record) => {
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
    const { tableContainerRef, scrollY } = usePinnedTableHeaderProps();

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
                                      <Table
                                          columns={assertionsTableCols.slice(0, -1)}
                                          dataSource={record?.assertions || []}
                                          pagination={false}
                                          showHeader={false}
                                          rowClassName={rowClassName}
                                          onRow={onRowClick}
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
