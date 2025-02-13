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
}

const ProposalsTable = ({ actionRequests, isLoading, onActionRequestUpdate }: Props) => {
    const columns = useGetColumns({ onActionRequestUpdate });

    return (
        <TableContainer>
            <Table columns={columns} data={actionRequests} isLoading={isLoading} isScrollable />
        </TableContainer>
    );
};

export default ProposalsTable;
