import { Button } from '@components';
import { Modal } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import Query from '@app/entityV2/shared/tabs/Dataset/Queries/Query';

const Footer = styled.div`
    display: flex;
    justify-content: flex-end;
`;

type Props = {
    logic: string;
    visible: boolean;
    onClose: () => void;
};

export const DatasetAssertionLogicModal = ({ logic, visible, onClose }: Props) => {
    const { t: tc } = useTranslation('common.actions');
    return (
        <Modal
            visible={visible}
            onCancel={onClose}
            footer={
                <Footer>
                    <Button variant="outline" color="gray" onClick={onClose}>
                        {tc('close')}
                    </Button>
                </Footer>
            }
        >
            <Query query={logic} />
        </Modal>
    );
};
