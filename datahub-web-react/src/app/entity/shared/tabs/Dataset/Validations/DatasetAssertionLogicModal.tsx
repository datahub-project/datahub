import { Modal, Button } from 'antd';
import React from 'react';
import Query from '../Queries/Query';

export type AssertionsSummary = {
    totalAssertions: number;
    totalRuns: number;
    failedRuns: number;
    succeededRuns: number;
};

type Props = {
    logic: string;
    open: boolean;
    onClose: () => void;
};

export const DatasetAssertionLogicModal = ({ logic, open, onClose }: Props) => {
    return (
        <Modal open={open} onCancel={onClose} footer={<Button onClick={onClose}>Close</Button>}>
            <Query query={logic} />
        </Modal>
    );
};
