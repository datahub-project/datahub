import React, { useState } from 'react';
import { Modal, Button } from 'antd';
import { getModalDomContainer } from '@utils/focus';
import { ModalButtonContainer } from '@app/shared/button/styledComponents';
import { OrganizationPicker } from '@app/organization/OrganizationPicker';

interface Props {
    open: boolean;
    onClose: () => void;
    onAdd: (urns: string[]) => void;
}

export const AddOrganizationModal = ({ open, onClose, onAdd }: Props) => {
    const [urns, setUrns] = useState<string[]>([]);

    const handleOk = () => {
        onAdd(urns);
        setUrns([]);
        onClose();
    };

    const handleCancel = () => {
        setUrns([]);
        onClose();
    };

    return (
        <Modal
            title="Add Organization"
            open={open}
            onCancel={handleCancel}
            footer={
                <ModalButtonContainer>
                    <Button type="text" onClick={handleCancel}>
                        Cancel
                    </Button>
                    <Button type="primary" onClick={handleOk} disabled={urns.length === 0}>
                        Add
                    </Button>
                </ModalButtonContainer>
            }
            getContainer={getModalDomContainer}
        >
            <OrganizationPicker
                selectedUrns={urns}
                onChange={setUrns}
            />
        </Modal>
    );
};
