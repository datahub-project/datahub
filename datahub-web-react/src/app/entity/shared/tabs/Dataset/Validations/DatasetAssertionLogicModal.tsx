/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Button, Modal } from 'antd';
import React from 'react';

import Query from '@app/entity/shared/tabs/Dataset/Queries/Query';

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
