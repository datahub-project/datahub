import React from 'react';
import styled from 'styled-components';

import EmptyProposals from '@app/taskCenterV2/proposalsV2/proposalsTable/EmptyProposals';
import { useGetColumns } from '@app/taskCenterV2/proposalsV2/proposalsTable/useGetColumns';
import { Table } from '@src/alchemy-components';
import { ActionRequest } from '@src/types.generated';

const TableContainer = styled.div`
    display: flex;
    overflow: auto;
    flex: 1;
`;

interface Props {
    actionRequests: ActionRequest[];
    isLoading: boolean;
    enableSelection?: boolean;
    isRowSelectionDisabled: (record: ActionRequest) => boolean;
    onActionRequestUpdate: (completedUrns: string[]) => void;
    selectedKeys: string[];
    setSelectedProposals: React.Dispatch<React.SetStateAction<ActionRequest[]>>;
    setSelectedKeys: React.Dispatch<React.SetStateAction<string[]>>;
    onRowClick?: (record: ActionRequest) => void;
    showPendingView?: boolean;
    showAssignee?: boolean;
}

const ProposalsTable = ({
    actionRequests,
    isLoading,
    enableSelection = true,
    isRowSelectionDisabled,
    onActionRequestUpdate,
    selectedKeys,
    setSelectedKeys,
    setSelectedProposals,
    onRowClick,
    showPendingView,
    showAssignee,
}: Props) => {
    const columns = useGetColumns({ onActionRequestUpdate, showPendingView, showAssignee });

    if (!isLoading && actionRequests.length === 0) {
        return <EmptyProposals />;
    }

    return (
        <TableContainer data-testid="proposals-table-container">
            <Table
                columns={columns}
                data={actionRequests}
                isLoading={isLoading}
                rowKey={(record) => record.urn}
                rowSelection={
                    enableSelection
                        ? {
                              selectedRowKeys: selectedKeys,
                              onChange: (keys, rows) => {
                                  setSelectedKeys(keys);
                                  setSelectedProposals(rows);
                              },
                              getCheckboxProps: (record) => ({
                                  disabled: isRowSelectionDisabled(record),
                              }),
                          }
                        : undefined
                }
                onRowClick={onRowClick}
                style={{ tableLayout: 'auto' }}
                isScrollable
                data-testid="proposals-table"
            />
        </TableContainer>
    );
};

export default ProposalsTable;
