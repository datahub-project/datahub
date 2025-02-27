import { Table } from '@src/alchemy-components';
import { ActionRequest } from '@src/types.generated';
import React from 'react';
import styled from 'styled-components';
import { useGetColumns } from './useGetColumns';

const TableContainer = styled.div`
    margin: 20px 20px 0 20px;
    display: flex;
    overflow: auto;
    flex: 1;
`;

interface Props {
    actionRequests: ActionRequest[];
    isLoading: boolean;
    onActionRequestUpdate: () => void;
    selectedKeys: string[];
    setSelectedKeys: React.Dispatch<React.SetStateAction<string[]>>;
}

const ProposalsTable = ({ actionRequests, isLoading, onActionRequestUpdate, selectedKeys, setSelectedKeys }: Props) => {
    const columns = useGetColumns({ onActionRequestUpdate });

    return (
        <TableContainer>
            <Table
                columns={columns}
                data={actionRequests}
                isLoading={isLoading}
                isScrollable
                rowKey={(record) => record.urn}
                rowSelection={{
                    selectedRowKeys: selectedKeys,
                    onChange: (keys) => setSelectedKeys(keys),
                }}
            />
        </TableContainer>
    );
};

export default ProposalsTable;
