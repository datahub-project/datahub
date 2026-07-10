import { Button, Modal } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';

import Query from '@app/entityV2/shared/tabs/Dataset/Queries/Query';

type Props = {
    logic: string;
    title?: string;
    description?: string;
    visible: boolean;
    onClose: () => void;
};

export const DatasetAssertionLogicModal = ({ logic, title, description, visible, onClose }: Props) => {
    const { t: tc } = useTranslation('common.actions');
    return (
        <Modal visible={visible} onCancel={onClose} footer={<Button onClick={onClose}>{tc('close')}</Button>}>
            <Query query={logic} title={title} description={description} />
        </Modal>
    );
};
