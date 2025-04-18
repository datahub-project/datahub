import { Modal } from 'antd';
import React from 'react';

import { SnowflakeConnectionForm } from '@app/settings/platform/snowflake/Form';

interface Props {
    title: string;
    snowflakeConnectionId?: string;
    isModalVisible: boolean;
    closeModal: () => void;
}

export const SnowflakeConnectionModal = ({ title, snowflakeConnectionId, isModalVisible, closeModal }: Props) => (
    <Modal title={title} open={isModalVisible} onCancel={closeModal} footer={null}>
        <SnowflakeConnectionForm snowflakeConnectionId={snowflakeConnectionId} postSave={closeModal} />
    </Modal>
);
