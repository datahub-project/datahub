import { Button, Modal } from 'antd';
import React from 'react';

import Query from '@app/entityV2/shared/tabs/Dataset/Queries/Query';

export type AssertionsSummary = {
    totalAssertions: number;
    totalRuns: number;
    failedRuns: number;
    succeededRuns: number;
};

type Props = {
    logic: string;
    visible: boolean;
    onClose: () => void;
};

export const DatasetAssertionLogicModal = ({ logic, visible, onClose }: Props) => {
    return (
        <Modal visible={visible} onCancel={onClose} footer={<Button onClick={onClose}>Close</Button>}>
            <Query query={logic} />
        </Modal>
    );
};
