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
    isRowSelectionDisabled: (record: ActionRequest) => boolean;
    onActionRequestUpdate: () => void;
    selectedKeys: string[];
    setSelectedKeys: React.Dispatch<React.SetStateAction<string[]>>;
    onRowClick?: (record: ActionRequest) => void;
    showPendingView?: boolean;
}

const ProposalsTable = ({
    actionRequests,
    isLoading,
    isRowSelectionDisabled,
    onActionRequestUpdate,
    selectedKeys,
    setSelectedKeys,
    onRowClick,
    showPendingView,
}: Props) => {
    const columns = useGetColumns({ onActionRequestUpdate, showPendingView });

    if (!isLoading && actionRequests.length === 0) {
        return <EmptyProposals />;
    }

    return (
        <TableContainer>
            <Table
                columns={columns}
                data={actionRequests}
                isLoading={isLoading}
                rowKey={(record) => record.urn}
                rowSelection={{
                    selectedRowKeys: selectedKeys,
                    onChange: (keys) => setSelectedKeys(keys),
                    getCheckboxProps: (record) => ({
                        disabled: isRowSelectionDisabled(record),
                    }),
                }}
                onRowClick={onRowClick}
                style={{ tableLayout: 'auto' }}
                isScrollable
            />
        </TableContainer>
    );
};

export default ProposalsTable;
