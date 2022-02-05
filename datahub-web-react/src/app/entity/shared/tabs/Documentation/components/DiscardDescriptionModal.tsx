import React from 'react';
import { Modal, Button } from 'antd';

type Props = {
    cancelModalVisible?: boolean;
    onDiscard?: () => void;
    onCancel?: () => void;
};

export const DiscardDescriptionModal = ({ cancelModalVisible, onDiscard, onCancel }: Props) => {
    return (
        <>
            <Modal
                title="Discard Changes"
                visible={cancelModalVisible}
                destroyOnClose
                onCancel={onCancel}
                footer={[
                    <Button type="text" onClick={onCancel}>
                        Cancel
                    </Button>,
                    <Button onClick={onDiscard}>Discard</Button>,
                ]}
            >
                <p>Changes will not be saved. Do you want to proceed?</p>
            </Modal>
        </>
    );
};
