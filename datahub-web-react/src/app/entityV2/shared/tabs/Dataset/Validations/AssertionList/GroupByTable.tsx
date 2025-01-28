import { ANTD_GRAY } from '@src/app/entityV2/shared/constants';
import React from 'react';
import styled from 'styled-components';
import { AssertionListTableRow } from './types';

const GroupContainer = styled.div`
    background: #fafafa;
    &&& .acryl-selected-assertions-table-row {
        background-color: ${ANTD_GRAY[4]};
    }
`;

const Row = styled.div`
    display: flex;
    width: 100%;
    border-bottom: 1px solid #f0f0f0;
    padding: 16px;
`;

const Cell = styled.div<{ width?: string; minWidth?: string | null; paddingRight?: string | number }>`
    width: ${(props) => props.width || 'auto'};
    min-width: ${(props) => props.minWidth || null};
    padding-right: ${(props) => props.paddingRight || '20px'};
    text-overflow: ellipsis;
    overflow-wrap: break-word;
`;

type AcrylAssertionGroupByComponentProps = {
    groupData: AssertionListTableRow[];
    columns: any[];
    onRow: (record: any) => {
        onClick: (_: any) => void;
    };
    focusUrn?: string;
    entityType: string;
};

export const GroupByTable = ({
    groupData,
    columns,
    onRow,
    focusUrn,
    entityType,
}: AcrylAssertionGroupByComponentProps) => {
    return (
        <GroupContainer>
            {groupData?.map((item) => (
                <Row
                    key={item.name}
                    onClick={(event) => {
                        onRow(item)?.onClick(event);
                    }}
                    className={
                        focusUrn === item.urn
                            ? `acryl-selected-${entityType}s-table-row`
                            : `acryl-${entityType}s-table-row`
                    }
                >
                    {columns.map((column) => {
                        const isNameColumn = column?.key === 'name';
                        const isActionColumn = column?.key === 'actions';
                        const minWidth = isNameColumn ? 'min-content' : null;
                        return (
                            column?.width &&
                            column?.key && (
                                <Cell
                                    key={column.key}
                                    width={column.width}
                                    style={{ display: !isNameColumn ? 'flex' : 'block', alignItems: 'center' }}
                                    minWidth={minWidth}
                                    paddingRight={isActionColumn ? '0px' : undefined}
                                >
                                    {column.render
                                        ? column.render(item[column.dataIndex], item)
                                        : item[column.dataIndex]}
                                </Cell>
                            )
                        );
                    })}
                </Row>
            ))}
        </GroupContainer>
    );
};
